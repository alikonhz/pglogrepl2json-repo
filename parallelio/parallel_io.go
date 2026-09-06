package parallelio

import (
	"context"
	"sync"

	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

type IORequest interface {
}

type TrackedIORequest struct {
	Request   IORequest
	WorkerID  uint32
	RequestID uint64
}

type IOHandler func(ctx context.Context, request TrackedIORequest) error

type ParallelIO struct {
	wg        sync.WaitGroup
	handler   IOHandler
	requestCh chan IORequest
	doneCh    chan struct{}
	logger    *zap.Logger
}

func NewParallelIO(ctx context.Context, handler IOHandler, numWorkers uint32, logger *zap.Logger) *ParallelIO {
	if numWorkers == 0 {
		numWorkers = 1
	}

	io := &ParallelIO{
		wg:        sync.WaitGroup{},
		handler:   handler,
		requestCh: make(chan IORequest, numWorkers),
		doneCh:    make(chan struct{}),
		logger:    logger.Named("parallel-io"),
	}

	io.processIO(ctx, numWorkers)

	return io
}

func (io *ParallelIO) processIO(ctx context.Context, numWorkers uint32) {
	var workerID uint32
	for workerID = 1; workerID <= numWorkers; workerID++ {
		io.wg.Add(1)

		go func(workerID uint32) {
			var requestID uint64 = 1

			defer func() {
				io.logger.Debug("processIO: worker finished", zap.Uint32("workerID", workerID))
				io.wg.Done()
			}()

			for {
				select {
				case request := <-io.requestCh:
					if err := io.handler(ctx, TrackedIORequest{
						Request:   request,
						WorkerID:  workerID,
						RequestID: requestID,
					}); err != nil {
						io.logger.Error("processIO: error", zap.Uint32("workerID", workerID), zap.Uint64("requestID", requestID),
							zap.Error(err))
					}
				case <-io.doneCh:
					return
				}

				requestID++
			}
		}(workerID)
	}
}

func (io *ParallelIO) Close() {
	close(io.doneCh)
	io.wg.Wait()
}

func (io *ParallelIO) Submit(req IORequest) error {
	io.requestCh <- req

	return nil
}

type ParallelIOBatchEntry struct {
	LSN       pglogrepl.LSN
	XID       uint32
	TableName pgschema.TableName
	PK        string
	Offset    uint32
}
