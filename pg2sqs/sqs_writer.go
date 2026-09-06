package pg2sqs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/parallelio"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/queuemapper"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsconfig"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsops"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/stats"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

const (
	workerIDParam  = "worker_id"
	requestIDParam = "request_id"
	batchSizeParam = "batch_size"
)

func newSqsWriter(sqsClient *sqs.Client,
	responseTracker pg2buffer.ResponseTracker,
	configTables map[string]*sqsconfig.SQSTableConfig,
	listenerConfig *SQSListenerOptions,
	logger *zap.Logger) *sqsWriter {
	closeCh := make(chan struct{})
	qm := queuemapper.NewSQSQueueMapper(configTables)
	w := &sqsWriter{
		sqsClient:       sqsClient,
		queueMapper:     qm,
		eventCh:         make(chan any, listenerConfig.WriterOpts.FlushOpts.QueueDepth),
		logger:          logger.Named("writer-sqs"),
		saveCommitTime:  listenerConfig.WriterOpts.TxTimeOpts.Save,
		commitTimeName:  listenerConfig.WriterOpts.TxTimeOpts.Name,
		closeCh:         closeCh,
		wg:              &sync.WaitGroup{},
		flushInterval:   listenerConfig.WriterOpts.FlushOpts.Interval,
		flushWorkers:    listenerConfig.WriterOpts.FlushOpts.Workers,
		sqsTimeout:      listenerConfig.WriterOpts.FlushOpts.Timeout,
		responseTracker: responseTracker,
		stats:           stats.NewSQSStats(listenerConfig.ListenerOpts.AppName, qm.GetAllQueueNames()),
	}

	return w
}

type sqsWriter struct {
	pg2stats.StatLogger

	sqsClient      *sqs.Client
	queueMapper    *queuemapper.SQSQueueMapper
	saveCommitTime bool
	commitTimeName string

	// for now only *sqsBatchEntry
	eventCh chan any

	closeCh chan struct{}
	wg      *sync.WaitGroup
	logger  *zap.Logger

	stats           *stats.SQSStats
	flushInterval   time.Duration
	flushWorkers    uint32
	sqsTimeout      time.Duration
	responseTracker pg2buffer.ResponseTracker

	parallelIO *parallelio.ParallelIO
}

func (sw *sqsWriter) GetStatsAsFields() []zap.Field {
	return sw.stats.GetStatsAsFields()
}

func (sw *sqsWriter) newSqsQueueBatchBuffer(queueConfig *queuemapper.TableQueueConfig) *sqsQueueBatchBuffer {
	return &sqsQueueBatchBuffer{
		queueConfig: queueConfig,
		logger:      sw.logger.Named("writer-sqs-batch"),
		sqsClient:   sw.sqsClient,
		stats:       sw.stats,
		entries:     make([]*sqsBatchEntry, 0),
		timeout:     sw.sqsTimeout,
	}
}

func (sw *sqsWriter) GracefulShutdown(_ context.Context) {
	// stop the worker (runWorker)
	close(sw.closeCh)

	if sw.parallelIO != nil {
		// wait until parallelIO is done
		sw.parallelIO.Close()
	}

	sw.logger.Debug("waiting for SQS writer to stop")
	sw.wg.Wait()
}

var (
	ErrTableConfigNotFound   = errors.New("table configuration not found")
	ErrTableNotInPub         = errors.New("groupID error: table does not exist in publication")
	ErrUnexpectedRequestType = errors.New("unexpected request type")
	ErrSendMessageBatch      = errors.New("SendMessageBatch error")
	ErrSQSMarshalJSON        = errors.New("failed to marshal JSON")
)

func (sw *sqsWriter) ValidateConfig(_ context.Context, pubTables map[string][]string) error {
	var (
		err          error
		processedMap = map[string]bool{}
	)

	for qc := range sw.queueMapper.AllConfigs() {
		if _, processed := processedMap[qc.TableName()]; processed {
			continue
		}

		processedMap[qc.TableName()] = true
		columns, exists := pubTables[qc.TableName()]

		if !exists {
			err = errors.Join(err, fmt.Errorf("%w: %q", ErrTableNotInPub, qc.TableName()))

			continue
		}

		valErr := qc.ValidateMessageGroupID(columns)
		if valErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: %q", valErr, qc.TableName()))
		}
	}

	return err
}

func (sw *sqsWriter) handleError(err error) {
	// quit when one of the following errors:
	// ErrTableConfigNotFound
	// ErrUnexpectedRequestType
	// somehow track number of the following errors
	// if they reappear - then quit
	// ErrSendMessageBatch
	sw.logger.Error("SQS writer error", zap.Error(err))

	if errors.Is(err, circuitbreaker.ErrOpened) {
		sw.logger.Fatal("circuit breaker is opened", zap.Error(err))
	}
}

func (sw *sqsWriter) Start(ctx context.Context) {
	sw.wg.Add(1)
	go sw.runWorker(ctx)
}

func (sw *sqsWriter) ioHandler(ctx context.Context, req parallelio.TrackedIORequest) error {
	buffer, _ := req.Request.(*sqsQueueBatchBuffer)

	return buffer.flushBatch(ctx, flushRequest{
		workerID:        req.WorkerID,
		requestID:       req.RequestID,
		responseTracker: sw.responseTracker,
	})
}

func (sw *sqsWriter) runWorker(ctx context.Context) {
	sw.parallelIO = parallelio.NewParallelIO(ctx, sw.ioHandler, sw.flushWorkers, sw.logger)

	defer sw.wg.Done()

	queueBatches := make(map[string]*sqsQueueBatchBuffer)

	tryFlushBatch := func(queueURL string) error {
		buffer, ok := queueBatches[queueURL]
		if !ok || buffer.isEmpty() {
			return nil
		}

		queueBatches[queueURL] = sw.newSqsQueueBatchBuffer(buffer.queueConfig)

		return sw.parallelIO.Submit(buffer)
	}

	tryFlushAll := func() error {
		for queueName := range queueBatches {
			if err := tryFlushBatch(queueName); err != nil {
				return err
			}
		}

		return nil
	}

	tickerDuration := sw.flushInterval
	ticker := time.NewTicker(tickerDuration)
	isTimerRunning := false

	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sw.closeCh:
			return
		case req := <-sw.eventCh:
			switch r := req.(type) {
			case *sqsBatchEntry:
				url := r.queueConfig.QueueURL()
				name := r.queueConfig.QueueName()

				batchBuffer, ok := queueBatches[url]
				if !ok {
					batchBuffer = sw.newSqsQueueBatchBuffer(r.queueConfig)
					queueBatches[url] = batchBuffer
				}

				if !isTimerRunning {
					isTimerRunning = true

					ticker.Reset(tickerDuration)
				}

				batchBuffer.append(r)

				if batchBuffer.isMaxedOut() {
					sw.logger.Debug("flushing queue by size: start")

					if err := tryFlushBatch(url); err != nil {
						sw.logger.Error("failed to flush queue", zap.Error(err), zap.String(sqsops.SQSQueueNameParam, name))
						sw.handleError(err)
					}

					sw.logger.Debug("flushing queue by size: end")
				}
			default:
				sw.handleError(fmt.Errorf("%w: %v", ErrUnexpectedRequestType, r))
			}
		case <-ticker.C:
			isTimerRunning = false

			ticker.Stop()
			sw.logger.Debug("flushing all queues by timer: start")

			if err := tryFlushAll(); err != nil {
				sw.handleError(err)
			}

			sw.logger.Debug("flushing all queues by timer: end")
		}
	}
}

func (sw *sqsWriter) ping(ctx context.Context) error {
	for qc := range sw.queueMapper.AllConfigs() {
		queueURL, fifo, err := sqsops.ReadOrCreateSQSQueueURL(ctx, sw.sqsClient, qc.QueueName(), sw.logger)
		if err != nil {
			return err //nolint:wrapcheck
		}

		qc.Update(*queueURL, fifo)
	}

	return nil
}

func (sw *sqsWriter) getJSON(entry *pgwal.WriteEntry, commitTime time.Time, needsOpInJSON bool) (string, error) {
	custom := make([]orderedmap.ValuePair, 0, 2)
	if sw.saveCommitTime {
		commitTimeValue := commitTime.Format(time.RFC3339Nano)
		if _, exists := entry.Tuple.Get(sw.commitTimeName); exists {
			entry.Tuple.Set(sw.commitTimeName, commitTimeValue)
		} else {
			custom = append(custom, orderedmap.ValuePair{
				Key:   sw.commitTimeName,
				Value: commitTimeValue,
			})
		}
	}

	if needsOpInJSON {
		kind := ""
		switch entry.Kind {
		case pgwal.Insert:
			kind = "insert"
		case pgwal.Update:
			kind = "update"
		case pgwal.Delete:
			kind = "delete"
		}

		if kind != "" {
			if _, exists := entry.Tuple.Get("kind"); exists {
				entry.Tuple.Set("kind", kind)
			} else {
				custom = append(custom, orderedmap.ValuePair{
					Key:   "kind",
					Value: kind,
				})
			}
		}
	}

	j, err := entry.Tuple.CustomMarshalJSON(nil, custom)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrSQSMarshalJSON, err)
	}

	return string(j), nil
}

func (sw *sqsWriter) enqueueOrWrite(_ context.Context, req *pgwal.WriteRequest, tr pg2buffer.ResponseTracker) error {

	if len(req.Entries) == 0 {
		// it can be a heartbeat message or another empty tx, which must be acknowledged
		// in this case we immediately acknowledge the request
		tr.OnSuccess(&pgwal.Response{Entries: []*pgwal.ResponseEntry{
			&pgwal.ResponseEntry{
				LSN: req.LSN,
				XID: req.XID,
			},
		}})

		return nil
	}

	for _, entry := range req.Entries {
		tableName := entry.Table

		tableCfg := sw.queueMapper.GetQueueConfig(entry)
		if tableCfg == nil {
			err := fmt.Errorf("%w: %q", ErrTableConfigNotFound, tableName)
			sw.handleError(err)

			return err
		}

		j, err := sw.getJSON(entry, req.CommitTime, tableCfg.NeedsOpInJSON())
		if err != nil {
			return err
		}

		sw.logger.Debug("enqueueing message: start", zap.Uint32(pglogger.XIDParam, req.XID), zap.String("table", tableName.FullName), zap.String(pglogger.PKParam, entry.PK))
		sw.eventCh <- &sqsBatchEntry{
			ParallelIOBatchEntry: parallelio.ParallelIOBatchEntry{
				LSN:       req.LSN,
				XID:       req.XID,
				TableName: tableName,
				PK:        entry.PK,
				Offset:    entry.Offset,
			},
			message:         j,
			queueConfig:     tableCfg,
			deduplicationID: tableCfg.GetDeduplicationID(entry, req.XID),
			groupID:         tableCfg.GetMessageGroupID(entry, req.XID),
		}
		sw.logger.Debug("enqueueing message: end", zap.Uint32(pglogger.XIDParam, req.XID), zap.String("table", tableName.FullName), zap.String(pglogger.PKParam, entry.PK))
	}

	return nil
}

type sqsBatchEntry struct {
	parallelio.ParallelIOBatchEntry
	queueConfig     *queuemapper.TableQueueConfig
	message         string
	deduplicationID *string
	groupID         *string
}

type flushRequest struct {
	workerID        uint32
	requestID       uint64
	responseTracker pg2buffer.ResponseTracker
}

type flushError struct {
	id          string
	code        string
	message     string
	senderFault bool
}

func (fe *flushError) Error() string {
	return fmt.Sprintf("%s: %s", fe.code, fe.message)
}

func (fe *flushError) IsRetryable() bool {
	return !fe.senderFault
}

func (fe *flushError) IsDownStreamDown() bool {
	return false
}

type sqsSendError struct {
	reason string
}

func (se *sqsSendError) Error() string {
	return se.reason
}

func (se *sqsSendError) IsRetryable() bool {
	return true
}

func (se *sqsSendError) IsDownStreamDown() bool {
	return true
}

const maxBatchSizeInSQS = 10 // according to AWS docs

type sqsQueueBatchBuffer struct {
	sqsClient   *sqs.Client
	stats       *stats.SQSStats
	logger      *zap.Logger
	entries     []*sqsBatchEntry
	queueConfig *queuemapper.TableQueueConfig
	timeout     time.Duration
}

func (b *sqsQueueBatchBuffer) append(entry *sqsBatchEntry) {
	b.entries = append(b.entries, entry)
}

func (b *sqsQueueBatchBuffer) isEmpty() bool {
	return len(b.entries) == 0
}

func (b *sqsQueueBatchBuffer) isMaxedOut() bool {
	return len(b.entries) >= maxBatchSizeInSQS
}

func (b *sqsQueueBatchBuffer) flushBatch(ctx context.Context, req flushRequest) error {
	b.logger.Debug("batch start", zap.Int(batchSizeParam, len(b.entries)), zap.Uint32(workerIDParam, req.workerID),
		zap.Uint64(requestIDParam, req.requestID))

	var (
		currentBatch   = make([]types.SendMessageBatchRequestEntry, maxBatchSizeInSQS)
		currentEntries = b.entries

		maxLSN pglogrepl.LSN
		maxXID uint32
	)

	b.entries = nil

	batchSize := len(currentEntries)

	batchMap := map[string]*sqsBatchEntry{}

	for i, batchEntry := range currentEntries {
		if batchEntry.LSN > maxLSN {
			maxLSN = batchEntry.LSN
		}

		if batchEntry.XID > maxXID {
			maxXID = batchEntry.XID
		}

		id := uuid.New().String()
		batchMap[id] = batchEntry

		b.logger.Debug("batch entry", zap.Uint32(pglogger.XIDParam, batchEntry.XID),
			zap.String(pglogger.PKParam, batchEntry.PK),
			zap.String(pglogger.TableParam, batchEntry.TableName.String()),
		)

		b.logger.Debug(batchEntry.message)

		currentBatch[i] = types.SendMessageBatchRequestEntry{ //nolint
			Id:                     aws.String(id),
			MessageBody:            aws.String(batchEntry.message),
			MessageDeduplicationId: batchEntry.deduplicationID,
			MessageGroupId:         batchEntry.groupID,
		}
	}

	maxLSNStr := maxLSN.String()

	b.logger.Debug("SendMessageBatch start", zap.String(sqsops.SQSQueueNameParam, b.queueConfig.QueueName()),
		zap.Uint32(workerIDParam, req.workerID),
		zap.Uint64(requestIDParam, req.requestID),
		zap.String(pglogger.LSNParam, maxLSNStr),
	)

	b.stats.RequestsTotal.Add(b.queueConfig.QueueName(), float64(batchSize))
	b.stats.TxLastSent.Set(float64(maxXID))
	b.stats.RequestsInflightTotal.Add(1)

	t1 := time.Now()

	childCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	sqsResponse, err := b.sqsClient.SendMessageBatch(childCtx, &sqs.SendMessageBatchInput{
		Entries:  currentBatch[:batchSize],
		QueueUrl: aws.String(b.queueConfig.QueueURL()),
	})

	t2 := time.Now()

	taken := t2.Sub(t1)

	b.stats.RequestsInflightTotal.Add(-1)

	if err != nil {
		wrappedErr := fmt.Errorf("%w: %w", ErrSendMessageBatch, err)

		b.processFailureFromSend(currentEntries, req, maxLSNStr, wrappedErr)

		return wrappedErr
	}

	b.logger.Debug("SendMessageBatch end", zap.String(sqsops.SQSQueueNameParam, b.queueConfig.QueueName()),
		zap.Uint32(workerIDParam, req.workerID),
		zap.Uint64(requestIDParam, req.requestID),
		zap.String(pglogger.LSNParam, maxLSNStr),
		zap.Duration("time_taken", taken),
	)

	if len(sqsResponse.Failed) > 0 {
		b.processFailuresFromSQSResponse(sqsResponse, req, batchMap)
	}

	okEntries := make([]*pgwal.ResponseEntry, len(batchMap))
	index := 0

	maxXID = 0

	for _, batchEntry := range batchMap {
		if batchEntry.XID > maxXID {
			maxXID = batchEntry.XID
		}

		okEntries[index] = &pgwal.ResponseEntry{
			PK:     batchEntry.PK,
			Table:  batchEntry.TableName,
			LSN:    batchEntry.LSN,
			XID:    batchEntry.XID,
			Offset: batchEntry.Offset,
		}
		index++
	}

	b.stats.TxLastOK.Set(float64(maxXID))

	req.responseTracker.OnSuccess(&pgwal.Response{Entries: okEntries})

	b.logger.Debug("batch end", zap.Int(batchSizeParam, len(currentEntries)),
		zap.Uint32(workerIDParam, req.workerID),
		zap.Uint64(requestIDParam, req.requestID),
		zap.String(pglogger.LSNParam, maxLSNStr),
	)

	return nil
}

func (b *sqsQueueBatchBuffer) processFailuresFromSQSResponse(sqsResponse *sqs.SendMessageBatchOutput, req flushRequest, batchMap map[string]*sqsBatchEntry) {
	var errEntries []*pgwal.ErrorResponseEntry

	b.stats.FailuresTotal.Add(b.queueConfig.QueueName(), float64(len(sqsResponse.Failed)))

	for _, failedEntry := range sqsResponse.Failed {
		b.logger.Error("SendMessageBatch failed entry", zap.String(sqsops.SQSQueueNameParam, b.queueConfig.QueueName()),
			zap.Uint32(workerIDParam, req.workerID),
			zap.Uint64(requestIDParam, req.requestID),
			zap.String(sqsops.SQSMessageIDParam, *failedEntry.Id),
			zap.String(sqsops.SQSErrorCodeParam, *failedEntry.Code),
			zap.String(sqsops.SQSErrorMessageParam, *failedEntry.Message),
			zap.Bool("sender_error", failedEntry.SenderFault),
		)

		id := *failedEntry.Id
		if entry, ok := batchMap[id]; ok {
			b.logger.Debug("SendMessageBatch failed entry (details)", zap.Uint32(pglogger.XIDParam, entry.XID),
				zap.String(pglogger.PKParam, entry.PK),
				zap.String(pglogger.TableParam, entry.TableName.String()),
			)

			errEntries = append(errEntries, &pgwal.ErrorResponseEntry{
				ResponseEntry: pgwal.ResponseEntry{
					PK:     entry.PK,
					Table:  entry.TableName,
					LSN:    entry.LSN,
					XID:    entry.XID,
					Offset: entry.Offset,
				},
				Reason: &flushError{
					id:          *failedEntry.Id,
					code:        *failedEntry.Code,
					message:     *failedEntry.Message,
					senderFault: failedEntry.SenderFault,
				},
			})
		} else {
			// if SQS sent us response for the ID we didn't sent
			// we need to crash the process immediately
			b.logger.Fatal("got unknown ID from SQS",
				zap.String(sqsops.SQSQueueNameParam, b.queueConfig.QueueName()),
				zap.String(sqsops.SQSMessageIDParam, *failedEntry.Id),
			)
		}

		delete(batchMap, id)
	}

	req.responseTracker.OnError(&pgwal.ErrResponse{
		Entries: errEntries,
	})
}

func (b *sqsQueueBatchBuffer) processFailureFromSend(currentEntries []*sqsBatchEntry, req flushRequest, maxLSNStr string, err error) {
	b.logger.Error("SendMessageBatch error", zap.Error(err), zap.String(sqsops.SQSQueueNameParam, b.queueConfig.QueueName()),
		zap.Uint32(workerIDParam, req.workerID),
		zap.Uint64(requestIDParam, req.requestID),
		zap.String(pglogger.LSNParam, maxLSNStr))

	b.stats.FailuresTotal.Add(b.queueConfig.QueueName(), float64(len(currentEntries)))

	errEntries := make([]*pgwal.ErrorResponseEntry, len(currentEntries))

	for i, failedEntry := range currentEntries {
		errEntries[i] = &pgwal.ErrorResponseEntry{
			ResponseEntry: pgwal.ResponseEntry{
				PK:     failedEntry.PK,
				Table:  failedEntry.TableName,
				LSN:    failedEntry.LSN,
				XID:    failedEntry.XID,
				Offset: failedEntry.Offset,
			},
			Reason: &sqsSendError{
				reason: err.Error(),
			},
		}
	}

	req.responseTracker.OnError(&pgwal.ErrResponse{
		Entries: errEntries,
	})
}
