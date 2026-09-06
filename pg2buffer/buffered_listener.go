package pg2buffer

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/alikonhz/pglogrepl2json/jsonparsing"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pg2buffer/pglsntracker"
	"github.com/alikonhz/pglogrepl2json/pg2buffer/stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

const (
	ErrRelationNotFound = "relation not found"
	MsgHeartbeat        = "heartbeat"
)

var (
	_ = pglogrepl2json.ReplicationListener((*PGBufferedListener)(nil))
)

type ResponseTracker interface {
	OnSuccess(resp *pgwal.Response) pglogrepl2json.CommitPoint
	OnError(resp *pgwal.ErrResponse)
}

func NewListenerOptions(publication string, numMode pgconfig.PgNumericMode, appName string) ListenerOptions {
	return ListenerOptions{
		Publication: publication,
		NumericMode: convertNumericMode(numMode),
		AppName:     appName,
	}
}

func convertNumericMode(mode pgconfig.PgNumericMode) decode.NumericEncoding {
	switch mode {
	case pgconfig.PgNumericModeFloat:
		return decode.NumericEncodingFloat
	case pgconfig.PgNumericModeString:
		return decode.NumericEncodingString
	default:
		return decode.NumericEncodingDefault
	}
}

type PGWALWriter interface {
	Write(ctx context.Context, req *pgwal.WriteRequest, responseTracker ResponseTracker)
	Ping(ctx context.Context) error
	// GracefulShutdown stops all the activities and waits for all the workers to finish.
	GracefulShutdown(ctx context.Context) error
	SaveState(ctx context.Context, lsn pglogrepl.LSN) error
	// Close closes the writer.
	Close(ctx context.Context) error
	SaveSnapshotLSN(ctx context.Context, lsn string) error
	GetSnapshotLSN(ctx context.Context) (string, error)

	TxOptions() appconfig.TxCommitTimeOptions
}

type txList struct {
	entries *list.List
	keys    map[string]*list.Element
}

func newTxList() *txList {
	return &txList{
		entries: list.New(),
		keys:    make(map[string]*list.Element),
	}
}

func (tl *txList) Get(key string) (*pgwal.WriteEntry, bool) {
	val, ok := tl.keys[key]
	if !ok {
		return nil, false
	}

	return val.Value.(*pgwal.WriteEntry), true
}

func (tl *txList) Add(txEntryKey string, entry *pgwal.WriteEntry) {
	el := tl.entries.PushBack(entry)
	tl.keys[txEntryKey] = el
}

func (tl *txList) MoveKeyToEnd(key string) {
	el, ok := tl.keys[key]
	if !ok {
		return
	}

	tl.entries.MoveToBack(el)
}

func (tl *txList) Len() int {
	if tl.entries == nil {
		return 0
	}

	return tl.entries.Len()
}

type ListenerOptions struct {
	Publication string
	NumericMode decode.NumericEncoding
	AppName     string
}

type relationVersion struct {
	version uint64
	rel     *pglogrepl.RelationMessageV2
	keyMap  *keymap.KeyMap
}

type PGBufferedListener struct {
	io.Closer
	listenerConfig  ListenerOptions
	pgConnector     *pgconnector.PGConnector
	txCache         map[uint32]*txList
	relations       map[uint32]*relationVersion
	typeMap         *pgtype.Map
	tables          map[string]*pgwal.PGTableWithConfig
	arena           *keymap.Arena
	relationVersion uint64

	currentXID uint32

	writer     PGWALWriter
	lsnTracker *pglsntracker.PGBufferedLSNTracker
	logger     *zap.Logger
	ackMu      sync.RWMutex
	ackHandler func(*pgwal.Response, pglogrepl2json.CommitPoint)

	coordinatedShutdownCh chan error

	stats *stats.BufferedListenerStats
}

func (rl *PGBufferedListener) GetStatsAsFields() []zap.Field {
	return rl.stats.GetStatsAsFields()
}

func (rl *PGBufferedListener) Logger() *zap.Logger {
	return rl.logger
}

func (rl *PGBufferedListener) LoadPubTables(ctx context.Context) error {

	ver, err := rl.pgConnector.PrimaryVersion()
	if err != nil {
		return fmt.Errorf("failed to load publication tables: %w", err)
	}

	res, err := pgconnector.ExecPrimary(ctx, rl.pgConnector, func(ctx context.Context, conn *pgx.Conn) (pgschema.PubTableRes, error) {
		return pgschema.ReadPubTables(ctx, conn, rl.listenerConfig.Publication, ver)
	})

	if err != nil {
		if errors.Is(err, pgschema.ErrPubNotFound) {
			return fmt.Errorf("publication %s not found", rl.listenerConfig.Publication)
		}

		return fmt.Errorf("failed to read publication tables: %w", err)
	}

	err = rl.mergeConfigAndPubTables(res.Tables)
	if err != nil {
		return err
	}

	return nil
}

func NewListener(writer PGWALWriter,
	listenerOpts ListenerOptions,
	pgConnector *pgconnector.PGConnector,
	configTables map[string]*config.TableConfig,
	failureConfig circuitbreaker.RetryConfig,
	logger *zap.Logger) *PGBufferedListener {

	tables := map[string]*pgwal.PGTableWithConfig{}
	var tableNames []string
	for key, tableConfig := range configTables {
		tables[key] = &pgwal.PGTableWithConfig{
			Cfg:   tableConfig,
			Table: nil,
		}

		tableNames = append(tableNames, key)
	}

	coordinatedShutdownCh := make(chan error)
	st := stats.NewStats(listenerOpts.AppName, tableNames)

	return &PGBufferedListener{
		writer:         writer,
		lsnTracker:     pglsntracker.NewLSNTrackerWithConfigAndStats(failureConfig, coordinatedShutdownCh, st, logger),
		listenerConfig: listenerOpts,
		pgConnector:    pgConnector,
		txCache:        make(map[uint32]*txList),
		relations:      make(map[uint32]*relationVersion),
		arena:          keymap.GlobalKeyMapStringArena,
		typeMap:        pgtype.NewMap(),
		logger:         logger.Named("listener-buffered"),

		tables:     tables,
		currentXID: 0,

		coordinatedShutdownCh: coordinatedShutdownCh,
		stats:                 st,
	}
}

func (rl *PGBufferedListener) ResponseTracker() ResponseTracker {
	return rl
}

func (rl *PGBufferedListener) CoordinatedShutdownCh() <-chan error {
	return rl.coordinatedShutdownCh
}

func (rl *PGBufferedListener) WriteQueueSize() uint64 {
	return rl.lsnTracker.WriteQueueSize()
}

func (rl *PGBufferedListener) PluginArguments(pubName string) ([]string, error) {
	ver, err := rl.pgConnector.PrimaryVersion()
	if err != nil {
		return nil, err
	}

	if ver >= pgschema.V14 {
		return []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", pubName),
			"messages 'true'",
			"streaming 'true'",
		}, nil
	}

	return []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", pubName),
	}, nil
}

// OnTxBegin is called when BeginMessage is received from the replication connection.
func (rl *PGBufferedListener) OnTxBegin(msg *pglogrepl.BeginMessage) error {
	rl.currentXID = msg.Xid
	rl.logger.Debug("txbegin", zap.Uint32(pglogger.XIDParam, msg.Xid))

	return nil
}

// OnTxCommit is called when CommitMessage is received from the replication connection.
func (rl *PGBufferedListener) OnTxCommit(msg *pglogrepl.CommitMessage) error {
	rl.logger.Debug("txcommit", zap.String(pglogger.LSNParam, msg.TransactionEndLSN.String()), zap.Uint32(pglogger.XIDParam, rl.currentXID))

	if entries, ok := rl.txCache[rl.currentXID]; rl.currentXID > 0 {
		rl.sendToLSNTracker(entries, msg.TransactionEndLSN, rl.currentXID, msg.CommitTime)

		if ok {
			delete(rl.txCache, rl.currentXID)
		}

		rl.currentXID = 0
	}

	return nil
}

// OnStreamStart is called when StreamStartMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnStreamStart(msg *pglogrepl.StreamStartMessageV2) error {
	rl.logger.Debug("streamstart",
		zap.Uint32(pglogger.XIDParam, msg.Xid),
	)

	return nil
}

// OnStreamStop is called when StreamStopMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnStreamStop(_ *pglogrepl.StreamStopMessageV2) error {
	rl.logger.Debug("streamstop")

	return nil
}

// OnStreamCommit is called when StreamCommitMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnStreamCommit(msg *pglogrepl.StreamCommitMessageV2) error {
	rl.logger.Debug("streamcommit",
		zap.Time("committime", msg.CommitTime),
		zap.String(pglogger.LSNParam, msg.TransactionEndLSN.String()),
		zap.Uint32(pglogger.XIDParam, msg.Xid),
	)

	entries, _ := rl.txCache[msg.Xid]

	rl.sendToLSNTracker(entries, msg.CommitLSN, msg.Xid, msg.CommitTime)
	delete(rl.txCache, msg.Xid)

	rl.stats.TxStreamedCount.Inc()

	return nil
}

// OnStreamAbort is called when StreamAbortMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnStreamAbort(msg *pglogrepl.StreamAbortMessageV2) error {
	rl.logger.Debug("streamabort",
		zap.Uint32(pglogger.XIDParam, msg.Xid),
	)

	delete(rl.txCache, msg.Xid)

	return nil
}

func (rl *PGBufferedListener) xidFromMsgOrCurrent(xid uint32) uint32 {
	if xid == 0 {
		return rl.currentXID
	}

	return xid
}

// OnInsert is called when InsertMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnInsert(msg *pglogrepl.InsertMessageV2) error {
	xid := rl.xidFromMsgOrCurrent(msg.Xid)
	rl.logger.Debug("insert", zap.Uint32("relid", msg.RelationID), zap.Uint32(pglogger.XIDParam, xid))
	rel, ok := rl.relations[msg.RelationID]

	if !ok {
		return fmt.Errorf("%v: %d", ErrRelationNotFound, msg.RelationID)
	}

	return rl.appendToTxCache(xid, rel.rel, pgwal.Insert, func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error) {
		return jsonparsing.ReadTuple(msg.Tuple, rel.rel.Columns, rl.typeMap, rl.getReadIndexes(tableKey, rel.rel.Columns), rel.keyMap)
	}, func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error) {
		// no prev data for Insert
		return nil, nil
	})
}

// OnUpdate is called when UpdateMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnUpdate(msg *pglogrepl.UpdateMessageV2) error {
	xid := rl.xidFromMsgOrCurrent(msg.Xid)
	rl.logger.Debug("update", zap.Uint32("relid", msg.RelationID), zap.Uint32(pglogger.XIDParam, xid))
	rel, ok := rl.relations[msg.RelationID]

	if !ok {
		return fmt.Errorf("%v: %d", ErrRelationNotFound, msg.RelationID)
	}

	return rl.appendToTxCache(xid, rel.rel, pgwal.Update, func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error) {
		return jsonparsing.ReadTuple(msg.NewTuple, rel.rel.Columns, rl.typeMap, rl.getReadIndexes(tableKey, rel.rel.Columns), rel.keyMap)
	}, func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error) {
		return jsonparsing.ReadTuple(msg.OldTuple, rel.rel.Columns, rl.typeMap, rl.getReadIndexes(tableKey, rel.rel.Columns), rel.keyMap)
	})
}

// OnDelete is called when DeleteMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnDelete(msg *pglogrepl.DeleteMessageV2) error {
	xid := rl.xidFromMsgOrCurrent(msg.Xid)
	rl.logger.Debug("delete", zap.Uint32("relid", msg.RelationID), zap.Uint32(pglogger.XIDParam, xid))
	rel, ok := rl.relations[msg.RelationID]

	if !ok {
		return fmt.Errorf("%v: %d", ErrRelationNotFound, msg.RelationID)
	}

	return rl.appendToTxCache(xid, rel.rel, pgwal.Delete, func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error) {
		return jsonparsing.ReadTuple(msg.OldTuple, rel.rel.Columns, rl.typeMap, rl.getReadIndexes(tableKey, rel.rel.Columns), rel.keyMap)
	}, func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error) {
		// no prev data for Delete
		// even though for deletes we have only OldTuple
		// we consider it as "new" data
		return nil, nil
	},
	)
}

// OnLogicalDecodingMessage is called when LogicalDecodingMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnLogicalDecodingMessage(msg *pglogrepl.LogicalDecodingMessageV2) error {
	// we can receive heartbeat as a message
	// in this case we need to send the LSN and XID with the empty request to the downstream writer
	// the downstream writer may ignore the empty request or may do something with it
	// right now: SQS ignores the request and Redis saves the LSN in Redis
	if msg.Transactional && strings.EqualFold(msg.Prefix, rl.listenerConfig.AppName) &&
		len(msg.Content) > 0 &&
		rl.currentXID > 0 {
		content := string(msg.Content)
		if strings.EqualFold(content, MsgHeartbeat) {
			rl.lsnTracker.Add(&pgwal.WriteRequest{
				LSN: msg.LSN,
				XID: rl.currentXID,
			})

			rl.logger.Debug(MsgHeartbeat,
				zap.String(pglogger.LSNParam, msg.LSN.String()),
				zap.Uint32(pglogger.XIDParam, msg.Xid),
			)
		}
	}

	return nil
}

// OnTruncate is called when TruncateMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnTruncate(msg *pglogrepl.TruncateMessageV2) error {
	xid := rl.xidFromMsgOrCurrent(msg.Xid)
	rl.logger.Debug("truncate", zap.Uint32(pglogger.XIDParam, xid))

	return nil
}

// OnRelation is called when RelationMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnRelation(msg *pglogrepl.RelationMessageV2) error {
	rl.logger.Debug("relation", zap.Uint32("relid", msg.RelationID), zap.String("relname", msg.RelationName))
	rl.relationVersion++

	tableNameKey := pgschema.MakeTableName(msg.Namespace, msg.RelationName)

	err := rl.LoadPubTables(context.Background())
	if err != nil {
		rl.logger.Fatal("failed to update config for table " + msg.RelationName)
	}

	pgTable, exists := rl.getTable(tableNameKey)
	if !exists {
		rl.logger.Fatal(fmt.Sprintf("table %s was not found in publication", msg.RelationName))
	}

	version := rl.relationVersion

	var cols []string

	for _, col := range msg.Columns {
		if _, exists := pgTable.Table.Columns[col.Name]; exists {
			cols = append(cols, col.Name)
		}
	}

	txOpts := rl.writer.TxOptions()
	if txOpts.Save && !slices.Contains(cols, txOpts.Name) {
		cols = append(cols, txOpts.Name)
	}

	ver := strconv.FormatUint(version, 10)
	keyMap := keymap.New(cols...)
	rl.relations[msg.RelationID] = &relationVersion{
		rel:     msg,
		version: version,
		keyMap:  keyMap,
	}

	rl.arena.Add(tableNameKey.FullName, ver, keyMap)

	return nil
}

// OnOrigin is called when OriginMessage is received from the replication connection.
func (rl *PGBufferedListener) OnOrigin(msg *pglogrepl.OriginMessage) error {
	return nil
}

// OnType is called when TypeMessageV2 is received from the replication connection.
func (rl *PGBufferedListener) OnType(msg *pglogrepl.TypeMessageV2) error {
	return nil
}

func (rl *PGBufferedListener) CommitPos() pglogrepl2json.CommitPoint {
	return rl.lsnTracker.MaxAcknowledged()
}

func (rl *PGBufferedListener) SaveState(ctx context.Context, pointLSN pglogrepl.LSN) error {
	return rl.writer.SaveState(ctx, pointLSN)
}

func (rl *PGBufferedListener) Close(ctx context.Context) error {
	return rl.writer.Close(ctx)
}

func (rl *PGBufferedListener) GracefulShutdown(shutdownCtx context.Context) error {
	rl.logger.Info("stopping downstream writer")

	// at this point we should no longer receive any writes from the upstream
	// so we shutdown the downstream writer first
	// this should wait for any outgoing requests either to fail with timeout or to succeed
	err := rl.writer.GracefulShutdown(shutdownCtx)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		rl.logger.Warn("writer shutdown error", zap.Error(err))
	}

	rl.logger.Info("stopping LSN tracker")
	// stop lsnTracker to save the last commit point
	err = rl.lsnTracker.Stop()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		rl.logger.Warn("LSN tracker shutdown error", zap.Error(err))
	}

	return nil
}

func (rl *PGBufferedListener) Start(ctx context.Context) {
	go rl.startWritePipeline(ctx)
}

func (rl *PGBufferedListener) OnSnapshotBatch(_ context.Context, tableName pgschema.TableName, batch *pgwal.WriteRequest) error {
	rl.stats.SnapshotReadInc(tableName.FullName, float64(len(batch.Entries)))
	rl.sendWALRequestToLSNTracker(batch)

	return nil
}

func (rl *PGBufferedListener) SetSnapshotAckHandler(handler func(*pgwal.Response, pglogrepl2json.CommitPoint)) {
	rl.ackMu.Lock()
	defer rl.ackMu.Unlock()

	rl.ackHandler = handler
}

func (rl *PGBufferedListener) OnSuccess(resp *pgwal.Response) pglogrepl2json.CommitPoint {
	rl.ackMu.RLock()
	handler := rl.ackHandler
	rl.ackMu.RUnlock()

	maxAcked := rl.lsnTracker.OnSuccess(resp)

	if handler != nil {
		handler(resp, maxAcked)
	}

	return maxAcked
}

func (rl *PGBufferedListener) OnError(resp *pgwal.ErrResponse) {
	rl.lsnTracker.OnError(resp)
}

func (rl *PGBufferedListener) GetTable(tableName pgschema.TableName) *pgschema.Table {
	tn, ok := rl.tables[tableName.FullName]
	if !ok || tn == nil {
		return nil
	}

	return tn.Table
}

func (rl *PGBufferedListener) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return rl.writer.SaveSnapshotLSN(ctx, lsn)
}

func (rl *PGBufferedListener) GetSnapshotLSN(ctx context.Context) (string, error) {
	return rl.writer.GetSnapshotLSN(ctx)
}

func (rl *PGBufferedListener) startWritePipeline(ctx context.Context) {
	queue := rl.lsnTracker.Queue()

	for {
		wr, stopped := queue.Pop()

		if stopped || ctx.Err() != nil {
			return
		}

		rl.stats.WriteQueueDec()
		rl.writer.Write(ctx, wr, rl.ResponseTracker())
		if !wr.IsSnapshot() {
			rl.stats.DownStreamLastSentTx.Set(float64(wr.XID))
			rl.stats.DownStreamLastSentLSN.Set(float64(wr.LSN))
		}
	}
}

func (rl *PGBufferedListener) appendToTxCache(xid uint32,
	rel *pglogrepl.RelationMessageV2,
	kind pgwal.OpKind,
	getData func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error),
	getPrevData func(tableKey pgschema.TableName) (*orderedmap.OrderedMap, error),
) error {
	tableNameKey := pgschema.MakeTableName(rel.Namespace, rel.RelationName)
	pgTable, exists := rl.getTable(tableNameKey)

	data, err := getData(tableNameKey)
	if err != nil {
		return err
	}

	prevData, err := getPrevData(tableNameKey)

	if err != nil {
		return err
	}

	primaryKey := pgschema.CreatePK(pgTable.Table, data)

	txEntries, exists := rl.txCache[xid]
	if !exists {
		txEntries = newTxList()
	}

	txEntryKey := pgTable.Table.FullName() + ":" + primaryKey

	existingEntry, exists := txEntries.Get(txEntryKey)
	if exists {
		existingEntry.Tuple = data
		txEntries.MoveKeyToEnd(txEntryKey)
	} else {
		txEntries.Add(txEntryKey, &pgwal.WriteEntry{
			Tuple:     data,
			PrevTuple: prevData,
			Kind:      kind,
			PK:        primaryKey,
			Table:     pgTable.Table.Name,
			// we do not set Offset here.
			// we set it in the sendToLSNTracker func.
			Offset: uint32(0),
		})
	}

	rl.txCache[xid] = txEntries
	rl.logger.Debug("saved row to tx cache",
		zap.String(pglogger.TableParam, pgTable.FullName()),
		zap.String(pglogger.PKParam, primaryKey),
		zap.Uint32(pglogger.XIDParam, xid),
		zap.Uint8(pglogger.KindParam, uint8(kind)),
	)

	return nil
}

func (rl *PGBufferedListener) mergeConfigAndPubTables(pubTables map[string]*pgschema.Table) error {
	for _, pubTable := range pubTables {
		key := pubTable.FullName()
		var (
			tableWithConfig *pgwal.PGTableWithConfig
			exists          bool
		)

		if tableWithConfig, exists = rl.getTable(pubTable.Name); !exists {
			rl.tables[pubTable.Name.FullName] = &pgwal.PGTableWithConfig{
				Table: pubTable,
				Cfg:   nil,
			}
		} else if tableWithConfig.Cfg != nil {
			var err error
			pubTable, err = mergeConfigAndPubTable(pubTable, tableWithConfig.Cfg)

			if err != nil {
				return fmt.Errorf("failed to merge config and publication table %q: %w", key, err)
			}

			tableWithConfig.Table = pubTable
		}
	}

	var err error
	for tn, tableWithConfig := range rl.tables {
		if tableWithConfig.Table == nil {
			err = errors.Join(err, fmt.Errorf("table %q was not found in publication", tn))
		}
	}

	return err
}

func (rl *PGBufferedListener) getReadIndexes(table pgschema.TableName, columns []*pglogrepl.RelationMessageColumn) decode.ReadTupleConfig {
	var tableWithConfig *pgwal.PGTableWithConfig
	cfg := decode.ReadTupleConfig{
		ReadAll: true,
		Encoding: decode.EncodingFormat{
			Numeric: rl.listenerConfig.NumericMode,
			Binary:  decode.BinaryEncodingBase64,
		},
	}

	var exists bool
	if tableWithConfig, exists = rl.getTable(table); !exists {
		return cfg
	}

	if tableWithConfig.Cfg != nil && tableWithConfig.Cfg.AllColumns() {
		return cfg
	}

	index := 0
	cols := make([]int, len(columns))

	for readIndex, col := range columns {
		if _, exists := tableWithConfig.Table.Columns[col.Name]; exists {
			cols[index] = readIndex
			index++
		}
	}

	cfg.ReadAll = false
	cfg.ReadIndexes = cols[:index]

	return cfg
}

func mergeConfigAndPubTable(table *pgschema.Table, tableConfig *config.TableConfig) (*pgschema.Table, error) {
	// Validate that all specified columns exist in the publication table,
	// even if AllColumns() is true (e.g. when {pairs:*} is used with a condition column).
	for _, column := range tableConfig.Columns {
		if column == config.AllColumns {
			continue
		}
		if _, exists := table.Columns[column]; !exists {
			return nil, fmt.Errorf("config for table %q has column %q which was not found in the publication. "+
				"make sure app configuration contains correct column names", table.Name.FullName, column)
		}
	}

	if tableConfig.AllColumns() {
		return table, nil
	}

	mergedColumns := make(map[string]*pgschema.Column)
	for _, column := range tableConfig.Columns {
		if pubColumn, exists := table.Columns[column]; exists {
			mergedColumns[pubColumn.Name] = pubColumn
		}
	}

	return table.WithColumns(mergedColumns), nil
}

func (rl *PGBufferedListener) GetPubTableColumns() map[string][]string {
	pubTables := map[string][]string{}
	for _, pubTable := range rl.tables {
		pubTables[pubTable.FullName()] = slices.Collect(maps.Keys(pubTable.Table.Columns))
	}

	return pubTables
}

func (rl *PGBufferedListener) GetPubTables() map[string]*pgschema.Table {
	pubTables := make(map[string]*pgschema.Table)
	for name, tableWithConfig := range rl.tables {
		if tableWithConfig.Table != nil {
			pubTables[name] = tableWithConfig.Table
		}
	}
	return pubTables
}

func (rl *PGBufferedListener) GetTableOption(table pgschema.TableName, option string) (string, bool) {
	t, ok := rl.getTable(table)
	if !ok {
		return "", false
	}

	if t.Cfg == nil {
		return "", false
	}

	return t.Cfg.Option(option), true
}

func (rl *PGBufferedListener) getTable(tableKey pgschema.TableName) (*pgwal.PGTableWithConfig, bool) {
	var (
		tableWithConfig *pgwal.PGTableWithConfig
		exists          bool
	)

	tableWithConfig, exists = rl.tables[tableKey.FullName]

	return tableWithConfig, exists
}

func (rl *PGBufferedListener) sendToLSNTracker(txl *txList, lsn pglogrepl.LSN, xid uint32, commitTime time.Time) {
	rl.stats.TxCount.Inc()
	rl.stats.TxLastProcessed.Set(float64(xid))
	rl.stats.LSNLastProcessed.Set(float64(lsn))

	if txl != nil && txl.Len() > 0 {
		var offset uint32 = 0
		var entries = make([]*pgwal.WriteEntry, txl.Len())

		for e := txl.entries.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*pgwal.WriteEntry)
			entry.Offset = offset
			entries[offset] = entry

			offset++

			switch entry.Kind {
			case pgwal.Insert:
				rl.stats.InsertInc(entry.Table.FullName)
			case pgwal.Update:
				rl.stats.UpdateInc(entry.Table.FullName)
			case pgwal.Delete:
				rl.stats.DeleteInc(entry.Table.FullName)
			case pgwal.SnapshotRead:
				rl.logger.Fatal("snapshot read is not supposed to be sent to LSN tracker. Report the issue to the support")
			}
		}

		rl.sendWALRequestToLSNTracker(&pgwal.WriteRequest{
			Entries:    entries,
			LSN:        lsn,
			XID:        xid,
			CommitTime: commitTime,
		})
	} else {
		rl.sendWALRequestToLSNTracker(&pgwal.WriteRequest{
			Entries:    nil,
			LSN:        lsn,
			XID:        xid,
			CommitTime: commitTime,
		})
	}
}

func (rl *PGBufferedListener) sendWALRequestToLSNTracker(req *pgwal.WriteRequest) {
	rl.lsnTracker.Add(req)
}
