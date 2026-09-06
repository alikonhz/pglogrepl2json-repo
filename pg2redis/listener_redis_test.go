package pg2redis

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pgCont "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
)

const (
	kb100 = 100 * 1024
)

// postgres - alik2024!

// TODO:
// sudo journalctl -u [service_name] -e
// 1. Update/Delete (ordinary and streamed) -> NOTE: DONE
// 2. Redis
// 2.1 HSets -> DONE
// 2.2 Sets -> DONE
// 2.3 PubSub -> DONE
// 2.4 RedisStreams -> DONE
// 3. Save state to Redis (LSN) and make sure no updates are sent to Redis before that LSN -> DONE
// 3.1 Also, for each entries save commit time (e.g. lastupdated, etc.) -> DONE
// 3.2 update publication config when publication changes (reload tables, etc.) -> DONE
// 3.3 manage slot/publication -> DONE
// 3.4 autoreconnect in case of failure
// 3.4.1 Redis -> DONE
// 3.4.2 Postgres -> DONE
// 4. Configuration ideas:
// 4.1 Columns to be produced  -> DONE
// 4.x save lastupdated or not and column name -> DONE
// 4.x lastupdated format -> TODO, maybe v2, for now RFC3339Nano
// 4.x issue: have lastupdated configured, then delete entry from database -> lastupdated still remains in the Redis -> DONE
// 4.x config: option to choose Redis output mode
// 4.x skip transactions in case of error -> TODO: DOCUMENT these cases.
// 4.x configure when to skip transactions? -> TODO
// 4.x custom columns should be supported even for PG < 15
// 4.x check how UPDATE works in the SET, PUB/SUB and STREAM modes -> DONE
//		full json should be available check replica identity full/default
// 4.x DONE: check the following case:
//		1. start redis in docker, start the app, delete redis container, start new container
//			-> app is unable to reconnect (error: context deadline exceeded)
//			probably the error was because I haven't exposed TLS port (6479) :))))
//			DONE: check anyway
//		2. check if the same applies to Postgres
// 4.x we save last LSN into the Redis. We also need to read this value on startup and ignore all log entries -> DONE
// 4.x master/replica setup. Check if there's some kind of epoch or any other way to acknowledge
//	which type we are connected to (master/replica) -> DONE
// 4.x DONE: ReadPubTables should read/merge tables from config. I.e. custom columns should work on PG < 15
// 5. REPLICA IDENTITY FULL vs REPLICA IDENTITY other -> DONE (nothing needs to be changed)
// 5.1 partitioning ??? -> TODO
// 6. PG slave/follower/replica databases support via pg_replication_slot_advance -> TODO
// 7. Multiple Redis instances by e.g. consistent hashing -> TODO v2
// 8. Auto reload config (look into native AWS config options) -> TODO v2
// 9. TLS connection to Redis and PG -> DONE
// 9.x TODO: certificate password
// 9.x TODO: think about this (see code comments)
// 9.x DONE: test against PG versions and redis versions
// 10. ???
// ??? TODO: tracing with pg_emit_message. Idea is that some app can save tracing data and
// ??? TODO: and PG2REDIS (or anything else) would pick this up

// DOCS:
// * Min supported version of PG is 12
// * User must have replication permissions:
// ** alter user <user> replication
// * To use commitTimeColumn feature Postgres cluster must have "track_commit_timestamp" enabled
// ** to view current setting: show track_commit_timestamp
// ** to change it: alter system set track_commit_timestamp = on or edit config file directly
// * Owner: app
// ** To add a table to a publication, the invoking user must have ownership rights on the table.
// ** PG version >=15 supports publications with a subset of a tables' columns. And it's possible to specify individual columns
//
//	for the table. Primary key column(s) must be included.
//
// ** For PG version >= 12 and < 15 "columns" publication reads all columns from the Postgres. Application does the filtering in-flight.
//   - Beware when adding new columns with default values. In this case all rows in the table get the default value. But the logical replication won't catch this.
//     You need to explicitly UPDATE required rows. However, updating all rows would cause all of them to be sent via logical replication.
//   - PG 17:
//
// ** On the PRIMARY: standby_slot_names (https://www.postgresql.org/docs/17/runtime-config-replication.html#GUC-STANDBY-SLOT-NAMES)
//	https://www.postgresql.org/docs/17/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION
//  On the REPLICA: sync_replication_slots = on (https://www.postgresql.org/docs/17/runtime-config-replication.html#GUC-SYNC-REPLICATION-SLOTS)
//   	and hot_standby_feedback = on

func TestStartFromLSN(t *testing.T) {
	rt := &StartFromLSNTest{}
	rt.alterWriter = func(w RedisWriter) RedisWriter {
		rw, ok := w.(*redisCircuitBreakerWriter)
		if !ok {
			panic("invalid redis writer (expected circuit breaker)")
		}
		rsw := &redisSliceWriter{
			redisWriter: rw.redisWriter,
			writes:      make([]*pgwal.WriteRequest, 0),
		}

		rt.rsw = rsw
		return rsw
	}

	rt.Setup(t, defaultTestOpts, defaultHSETCfg)
	defer rt.TearDown()
	rt.tt1 = &TestTable{ID: 99, FieldInt: 12, FieldData: []byte{6, 7, 8}}
	rt.mustInsert(rt.tt1)

	row := rt.pool.QueryRow(context.Background(), "select pg_current_wal_lsn()::text")
	var lsn string
	err := row.Scan(&lsn)
	if err != nil {
		panic(err)
	}

	pgLSN, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		panic(err)
	}

	rt.runTestFromLSN(t, rt, pgLSN+1)
}

type StartFromLSNTest struct {
	pg2RedisTest
	rsw *redisSliceWriter
	tt1 *TestTable
	tt2 *TestTable
}

func (rt *StartFromLSNTest) act() {
	rt.tt2 = &TestTable{ID: 100, FieldInt: 15, FieldData: []byte{9, 10, 11}}
	rt.mustInsert(rt.tt2)
}

func (rt *StartFromLSNTest) assert(t *testing.T) {
	var notEmptyWrites []*pgwal.WriteRequest
	for _, write := range rt.rsw.writes {
		if len(write.Entries) > 0 {
			notEmptyWrites = append(notEmptyWrites, write)
		}
	}

	assert.Len(t, notEmptyWrites, 1)
	assert.Len(t, notEmptyWrites[0].Entries, 1)
	assert.Equal(t, fmt.Sprintf("%d", rt.tt2.ID), notEmptyWrites[0].Entries[0].PK)
}

func TestAllPgTypes(t *testing.T) {
	modes := []struct {
		commands   []redisconfig.RedisCommandConfig
		assertFunc func(t *testing.T, test *AllPGTypesTest)
	}{
		{commands: []redisconfig.RedisCommandConfig{{"HSET", "{%schema%}.{%table%}:{%pk%}", "{pairs:*}"}}, assertFunc: assertAllPGTypesHSet},
		{commands: []redisconfig.RedisCommandConfig{{"SET", "{%schema%}.{%table%}:{%pk%}", "{json:*}"}}, assertFunc: assertAllPGTypesSet},
		//appconfig.WriterPubSub,
		//appconfig.WriterStream,
	}

	for _, testRun := range modes {
		t.Run(fmt.Sprintf("%v", testRun.commands[0][0]), func(t *testing.T) {
			test := AllPGTypesTest{}
			test.assertFunc = testRun.assertFunc
			test.SetupAllPGTypesTest(t, testRun.commands, defaultTestOpts)
			defer test.TearDownAllPGTypes()
			test.runTest(t, &test)
		})
	}
}

var (
	defaultTestOpts = RedisListenerOptions{
		ListenerOpts: pg2buffer.ListenerOptions{
			NumericMode: decode.NumericEncodingString,
		},
		WriterOpts: RedisWriterOptions{
			TxTimeOpts: appconfig.NewTxCommitTimeOptions("lastupdated"),
		},
	}

	testOptsNoTxTime = RedisListenerOptions{
		ListenerOpts: pg2buffer.ListenerOptions{
			NumericMode: decode.NumericEncodingString,
		},
		WriterOpts: RedisWriterOptions{
			TxTimeOpts: appconfig.NewTxCommitTimeOptions(""),
		},
	}
)

type AllPGTypesTest struct {
	pg2RedisTest
	tt         *allPgType
	assertFunc func(t *testing.T, test *AllPGTypesTest)
}

func (rt *AllPGTypesTest) act() {
	rt.tt = &allPgType{
		col1:  1,
		col2:  2,
		col3:  3,
		col4:  "4.44444444",
		col5:  "5.5555",
		col6:  "6.5625",
		col7:  "7.77",
		col10: "8.88",

		col11: "0123456789",
		col12: "text",
		col13: "citext",

		col14: []byte{1, 2, 3, 4, 5},

		col15: "2024-06-12T13:45:58.328746Z",
		col16: "2024-06-12T13:45:58.328746+04:00",
		col17: "2024-05-03",
		col18: "12:34:56.777777",
		col19: "02:15:12-03:15",
		col20: "2 years 3 months 1 day 12 hours 59 min 10 sec",
		col21: true,
		col22: "happy",
		col23: struct {
			s     string
			point pgtype.Point
		}{s: "(1,-50)", point: pgtype.Point{P: pgtype.Vec2{X: 1, Y: -50}, Valid: true}},
		col24: struct {
			s    string
			line pgtype.Line
		}{s: "{5,10,-15}", line: pgtype.Line{A: 5, B: 10, C: -15, Valid: true}},
		col25: struct {
			s    string
			lseg pgtype.Lseg
		}{s: "[(1,1),(-2,-5.25)]", lseg: pgtype.Lseg{P: [2]pgtype.Vec2{{X: 1, Y: 1}, {X: -2, Y: -5.25}}, Valid: true}},
		col26: struct {
			s   string
			box pgtype.Box
		}{s: "(12,15),(-10,-8)", box: pgtype.Box{P: [2]pgtype.Vec2{{X: 12, Y: 15}, {X: -10, Y: -8}}, Valid: true}},
		col27: struct {
			s    string
			path pgtype.Path
		}{s: "((1,1),(2,2),(-5,-8.18))", path: pgtype.Path{Closed: true, P: []pgtype.Vec2{{X: 1, Y: 1}, {X: 2, Y: 2}, {X: -5, Y: -8.18}}, Valid: true}},
		col28: struct {
			s       string
			polygon pgtype.Polygon
		}{s: "((1,1),(2,2),(-5,-8.18))", polygon: pgtype.Polygon{P: []pgtype.Vec2{{X: 1, Y: 1}, {X: 2, Y: 2}, {X: -5, Y: -8.18}}, Valid: true}},
		col29: struct {
			s      string
			circle pgtype.Circle
		}{s: "<(1,5),3.14>", circle: pgtype.Circle{P: pgtype.Vec2{X: 1, Y: 5}, R: 3.14, Valid: true}},
		col30: struct {
			s    string
			cidr string
		}{s: "192.168.17.32/32", cidr: "192.168.17.32/32"},
		col31: struct {
			s    string
			inet string
		}{s: "10.1.2.3", inet: "10.1.2.3"},
		col32: struct {
			s       string
			macaddr net.HardwareAddr
		}{s: "08:00:2b:01:02:03", macaddr: createMAC("08:00:2b:01:02:03")},
		col33: struct {
			s        string
			macaddr8 net.HardwareAddr
		}{s: "08:00:2b:01:02:03:04:05", macaddr8: createMAC("08:00:2b:01:02:03:04:05")},
		col34: struct {
			s    string
			bits pgtype.Bits
		}{s: "101", bits: bitsFromString("101")},
		col37: "c38610e0-ce41-4182-b930-266723b89e51",
		col38: `<root><doc><elem1 data="hi" /></doc></root>`,
		col39: `{"k1": "v1", "k2": 2}`,
		col40: `{"k3": "v3", "k4": 4}`,
		col41: struct {
			s        string
			jsonpath string
		}{s: `$.values[*] ? (@ >= $min && @ <= $max)`, jsonpath: `$."values"[*]?(@ >= $"min" && @ <= $"max")`},
		col42: struct {
			s     string
			array pgtype.Array[int]
		}{s: "{1,2,3}", array: pgtype.Array[int]{Elements: []int{1, 2, 3}, Valid: true}},
		col43: struct {
			s     string
			array pgtype.Array[string]
		}{s: "{'test1','test2'}", array: pgtype.Array[string]{Elements: []string{"test1", "test2"}, Valid: true}},
		col44: struct{ s string }{s: "(10,-1)"},
		col45: struct{ s string }{s: "[10,20)"},
		col46: struct{ s string }{s: "[11123917380,22123917380)"},
		col47: struct{ s string }{s: "[11.1,22.2)"},
		col48: struct {
			s     string
			value string
		}{s: "[\"2010-01-01 14:30:05\",\"2010-01-01 15:30:00\")", value: "[2010-01-01 14:30:05, 2010-01-01 15:30:00)"},
		col49: struct {
			s     string
			value string
		}{s: "[\"2010-01-01 13:30:00+00\",\"2010-01-01 14:30:00+00\")", value: "[\"2010-01-01 14:30:00+01\",\"2010-01-01 15:30:00+01\")"},
		col50: struct{ s string }{s: "[2010-01-01,2020-01-01)"},
	}

	tx, err := rt.pool.Begin(context.Background())
	if err != nil {
		panic(err)
	}

	defer tx.Rollback(context.Background())

	row := tx.QueryRow(context.Background(),
		`INSERT INTO all_pg_types (col1, col2, col3, col4, col5, col6, col7, col10, 
									   col11, col12, col13,
									   col14,
						               col15, col16, col17, col18, col19, col20,
                                       col21,
                                       col22,
									   col23, col24, col25, col26, col27, col28, col29,
                                       col30, col31, col32, col33,
                                       col34, col37,
								       col38, col39, col40, col41,
col42, col43,
col44, 
col45, col46, col47, col48, col49, col50

)
	 VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
			 $9, $10, $11,
			 $12,
             $13, $14, $15, $16, $17, $18,
			 $19,
             $20,
	         $21, $22, $23, $24, $25, $26, $27,
             $28, $29, $30, $31,
             $32, $33,
			 $34, $35, $36, $37,
$38, $39,
$40, $41, $42, $43, $44, $45, $46 
)
	RETURNING id
`, rt.tt.col1, rt.tt.col2, rt.tt.col3,
		toDecimal(rt.tt.col4), toDecimal(rt.tt.col5), toDecimal(rt.tt.col6), toDecimal(rt.tt.col7), toDecimal(rt.tt.col10),
		rt.tt.col11, rt.tt.col12, rt.tt.col13,
		rt.tt.col14,
		rt.tt.col15, rt.tt.col16, rt.tt.col17, rt.tt.col18, rt.tt.col19, rt.tt.col20,
		rt.tt.col21,
		rt.tt.col22,
		rt.tt.col23.point, rt.tt.col24.line, rt.tt.col25.lseg, rt.tt.col26.box, rt.tt.col27.path, rt.tt.col28.polygon, rt.tt.col29.circle,
		rt.tt.col30.cidr, rt.tt.col31.inet, rt.tt.col32.macaddr, rt.tt.col33.macaddr8,
		rt.tt.col34.s, rt.tt.col37,
		rt.tt.col38, rt.tt.col39, rt.tt.col40, rt.tt.col41.s,
		rt.tt.col42.s, rt.tt.col43.s,
		rt.tt.col44.s, rt.tt.col45.s, rt.tt.col46.s, rt.tt.col47.s, rt.tt.col48.value, rt.tt.col49.value, rt.tt.col50.s,
	)
	var id int64
	err = row.Scan(&id)
	if err != nil {
		panic(err)
	}

	tx.Commit(context.Background())

	rt.tt.id = id
}

func bitsFromString(s string) pgtype.Bits {
	b := &pgtype.Bits{}
	err := b.Scan(s)
	if err != nil {
		panic(err)
	}

	return *b
}

func createMAC(s string) net.HardwareAddr {
	m, err := net.ParseMAC(s)
	if err != nil {
		panic(err)
	}

	return m
}

func toDecimal(str string) string {
	return str
}

func toTimeString(layout, s string) string {
	t, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}

	return t.UTC().Format(time.RFC3339Nano)
}

func (rt *AllPGTypesTest) assert(t *testing.T) {
	rt.assertFunc(t, rt)
}

func assertAllPGTypesHSet(t *testing.T, test *AllPGTypesTest) {
	res, err := test.redisClient.HGetAll(context.Background(), pgschema.MakeRowKey("public.all_pg_types", fmt.Sprintf("%d", test.tt.id))).Result()
	require.NoError(t, err)

	assert.NotEqual(t, 0, len(res))

	assert.Equal(t, fmt.Sprintf("%d", test.tt.col1), res["col1"])
	assert.Equal(t, fmt.Sprintf("%d", test.tt.col2), res["col2"])
	assert.Equal(t, fmt.Sprintf("%d", test.tt.col3), res["col3"])
	assert.Equal(t, test.tt.col4, res["col4"])
	assert.Equal(t, test.tt.col5, res["col5"])
	assert.Equal(t, test.tt.col6, res["col6"])
	assert.Equal(t, test.tt.col7, res["col7"])
	assert.Equal(t, fmt.Sprintf("$%s", test.tt.col10), res["col10"])

	assert.Equal(t, test.tt.col11, res["col11"])
	assert.Equal(t, test.tt.col12, res["col12"])
	assert.Equal(t, test.tt.col13, res["col13"])

	assert.Equal(t, decode.EncodeBinary(test.tt.col14, decode.BinaryEncodingBase64), res["col14"])

	assert.Equal(t, toTimeString(time.RFC3339Nano, test.tt.col15), res["col15"])
	assert.Equal(t, toTimeString(time.RFC3339Nano, test.tt.col16), res["col16"])
	assert.Equal(t, test.tt.col17, res["col17"])
	assert.Equal(t, test.tt.col18, res["col18"])
	assert.Equal(t, test.tt.col19, res["col19"])

	assert.Equal(t, "2 years 3 mons 1 day 12:59:10", res["col20"])

	assert.Equal(t, "1", res["col21"])
	assert.Equal(t, test.tt.col22, res["col22"])
	assert.Equal(t, test.tt.col23.s, res["col23"])
	assert.Equal(t, test.tt.col24.s, res["col24"])
	assert.Equal(t, test.tt.col25.s, res["col25"])
	assert.Equal(t, test.tt.col26.s, res["col26"])
	assert.Equal(t, test.tt.col27.s, res["col27"])
	assert.Equal(t, test.tt.col28.s, res["col28"])
	assert.Equal(t, test.tt.col29.s, res["col29"])

	assert.Equal(t, test.tt.col30.s, res["col30"])
	assert.Equal(t, test.tt.col31.s, res["col31"])
	assert.Equal(t, test.tt.col32.s, res["col32"])
	assert.Equal(t, test.tt.col33.s, res["col33"])

	assert.Equal(t, test.tt.col34.s, res["col34"])
	assert.Equal(t, test.tt.col37, res["col37"])

	assert.Equal(t, test.tt.col38, res["col38"])
	assert.Equal(t, test.tt.col39, res["col39"])
	assert.Equal(t, test.tt.col40, res["col40"])
	assert.Equal(t, test.tt.col41.jsonpath, res["col41"])

	assert.Equal(t, test.tt.col42.s, res["col42"])
	assert.Equal(t, test.tt.col43.s, res["col43"])
	assert.Equal(t, test.tt.col44.s, res["col44"])
	assert.Equal(t, test.tt.col45.s, res["col45"])
	assert.Equal(t, test.tt.col46.s, res["col46"])
	assert.Equal(t, test.tt.col47.s, res["col47"])
	assert.Equal(t, test.tt.col48.s, res["col48"])
	assert.Equal(t, test.tt.col49.s, res["col49"])
	assert.Equal(t, test.tt.col50.s, res["col50"])
}

func assertAllPGTypesSet(t *testing.T, test *AllPGTypesTest) {
	resJSON, err := test.redisClient.Get(context.Background(), pgschema.MakeRowKey("public.all_pg_types", fmt.Sprintf("%d", test.tt.id))).Result()
	require.NoError(t, err)

	var res map[string]any
	err = json.Unmarshal([]byte(resJSON), &res)
	require.NoError(t, err)
	if err != nil {
		return
	}

	//s2f64 := func(s string) float64 {
	//	f, _ := strconv.ParseFloat(s, 64)
	//	return f
	//}

	s2f64 := func(s string) string {
		return s
	}

	assert.NotEqual(t, 0, len(res))
	assert.Equal(t, float64(test.tt.col1), res["col1"], "col1")
	assert.Equal(t, float64(test.tt.col2), res["col2"], "col2")
	assert.Equal(t, float64(test.tt.col3), res["col3"], "col3")
	assert.Equal(t, s2f64(test.tt.col4), res["col4"], "col4")
	assert.Equal(t, s2f64(test.tt.col5), res["col5"], "col5")
	assert.Equal(t, s2f64(test.tt.col6), res["col6"], "col6")
	assert.Equal(t, s2f64(test.tt.col7), res["col7"], "col7")
	assert.Equal(t, fmt.Sprintf("$%s", test.tt.col10), res["col10"])

	assert.Equal(t, test.tt.col11, res["col11"], "col11")
	assert.Equal(t, test.tt.col12, res["col12"], "col12")
	assert.Equal(t, test.tt.col13, res["col13"], "col13")

	assert.Equal(t, decode.EncodeBinary(test.tt.col14, decode.BinaryEncodingBase64), res["col14"], "col14")

	assert.Equal(t, toTimeString(time.RFC3339Nano, test.tt.col15), res["col15"], "col15")
	assert.Equal(t, toTimeString(time.RFC3339Nano, test.tt.col16), res["col16"], "col16")
	assert.Equal(t, test.tt.col17, res["col17"], "col17")
	assert.Equal(t, test.tt.col18, res["col18"], "col18")
	assert.Equal(t, test.tt.col19, res["col19"], "col19")

	assert.Equal(t, "2 years 3 mons 1 day 12:59:10", res["col20"], "col20")

	assert.Equal(t, true, res["col21"], "col21")
	assert.Equal(t, test.tt.col22, res["col22"], "col22")
	assert.Equal(t, test.tt.col23.s, res["col23"], "col23")
	assert.Equal(t, test.tt.col24.s, res["col24"], "col24")
	assert.Equal(t, test.tt.col25.s, res["col25"], "col25")
	assert.Equal(t, test.tt.col26.s, res["col26"], "col26")
	assert.Equal(t, test.tt.col27.s, res["col27"], "col27")
	assert.Equal(t, test.tt.col28.s, res["col28"], "col28")
	assert.Equal(t, test.tt.col29.s, res["col29"], "col29")

	assert.Equal(t, test.tt.col30.s, res["col30"], "col30")
	assert.Equal(t, test.tt.col31.s, res["col31"], "col31")
	assert.Equal(t, test.tt.col32.s, res["col32"], "col32")
	assert.Equal(t, test.tt.col33.s, res["col33"], "col33")

	assert.Equal(t, test.tt.col34.s, res["col34"], "col34")
	assert.Equal(t, test.tt.col37, res["col37"], "col37")

	assert.Equal(t, test.tt.col38, res["col38"], "col38")
	assert.Equal(t, test.tt.col39, res["col39"], "col39")
	assert.Equal(t, test.tt.col40, res["col40"], "col40")
	assert.Equal(t, test.tt.col41.jsonpath, res["col41"], "col41")

	assert.Equal(t, test.tt.col42.s, res["col42"], "col42")
	assert.Equal(t, test.tt.col43.s, res["col43"], "col43")
	assert.Equal(t, test.tt.col44.s, res["col44"], "col44")
	assert.Equal(t, test.tt.col45.s, res["col45"], "col45")
	assert.Equal(t, test.tt.col46.s, res["col46"], "col46")
	assert.Equal(t, test.tt.col47.s, res["col47"], "col47")
	assert.Equal(t, test.tt.col48.s, res["col48"], "col48")
	assert.Equal(t, test.tt.col49.s, res["col49"], "col49")
	assert.Equal(t, test.tt.col50.s, res["col50"], "col50")
}

func (rt *AllPGTypesTest) SetupAllPGTypesTest(t *testing.T, commands []redisconfig.RedisCommandConfig, config RedisListenerOptions) {
	rt.Setup(t, config, commands, func(appConfig *redisconfig.RedisAppConfig) {
		rt.updateConfig(appConfig, commands)
	}, rt.createTables)
}

func (rt *AllPGTypesTest) updateConfig(appConf *redisconfig.RedisAppConfig, commands []redisconfig.RedisCommandConfig) {
	appConf.Tables = map[string]*redisconfig.RedisTableConfig{}
	appConf.Tables["public.all_pg_types"] = &redisconfig.RedisTableConfig{
		Commands: convertCommands(commands),
		FullName: "public.all_pg_types",
	}
}

func (rt *AllPGTypesTest) TearDownAllPGTypes() {
	_, err := rt.pool.Exec(context.Background(), "DROP TABLE IF EXISTS all_pg_types")

	if err != nil {
		panic(err)
	}

	_, err = rt.redisClient.Del(context.Background(), "public.all_pg_types").Result()
	if err != nil {
		panic(err)
	}
	rt.TearDown()
}

type allPgType struct {
	id   int64
	col1 int8
	col2 int32
	col3 int64
	col4 string // decimal
	col5 string // numeric
	col6 string // real
	col7 string // double precision

	col10 string // money

	col11 string // varchar(10)
	col12 string // text
	col13 string // citext

	col14 []byte // bytea

	col15 string // timestamp without time zone
	col16 string // timestamp with time zone
	col17 string // date
	col18 string // time without time zone
	col19 string // time with time zone
	col20 string // interval

	col21 bool // bool

	col22 string // mood (custom type enum)

	col23 struct {
		s     string
		point pgtype.Point
	} // point
	col24 struct {
		s    string
		line pgtype.Line
	} // line
	col25 struct {
		s    string
		lseg pgtype.Lseg
	} // lseg
	col26 struct {
		s   string
		box pgtype.Box
	} // box
	col27 struct {
		s    string
		path pgtype.Path
	} // path
	col28 struct {
		s       string
		polygon pgtype.Polygon
	} // polygon
	col29 struct {
		s      string
		circle pgtype.Circle
	} // circle
	col30 struct {
		s    string
		cidr string
	} // cidr
	col31 struct {
		s    string
		inet string
	} // inet
	col32 struct {
		s       string
		macaddr net.HardwareAddr
	} // macaddr
	col33 struct {
		s        string
		macaddr8 net.HardwareAddr
	} // macaddr8
	col34 struct {
		s    string
		bits pgtype.Bits
	} // bit(3)
	//col35 tsvector null,
	//col36 tsquery null,
	col37 string // uuid

	col38 string // xml
	col39 string // json
	col40 string // jsonb
	col41 struct {
		s        string
		jsonpath string
	} // jsonpath
	col42 struct {
		s     string
		array pgtype.Array[int]
	} // integer[]
	col43 struct {
		s     string
		array pgtype.Array[string]
	} // text[]

	col44 struct {
		s string
	} // complex

	col45 struct {
		s string
	} // int4range

	col46 struct {
		s string
	} // int8range
	col47 struct {
		s string
	} // numrange
	col48 struct {
		s     string
		value string
	} // tsrange
	col49 struct {
		s     string
		value string
	} // tstzrange
	col50 struct {
		s string
	} // daterange
}

func (rt *AllPGTypesTest) createTables(appConfig *redisconfig.RedisAppConfig) {
	_, err := rt.pool.Exec(context.Background(), "create extension if not exists citext")
	if err != nil {
		panic(err)
	}

	// type can exist - ignore the error
	rt.pool.Exec(context.Background(), "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
	rt.pool.Exec(context.Background(), "CREATE TYPE complex AS (r double precision,i double precision)")

	_, err = rt.pool.Exec(context.Background(),
		`
CREATE TABLE IF NOT EXISTS all_pg_types(
	id bigserial not null primary key,
	col1 smallint null,
	col2 integer null,
	col3 bigint null,
	col4 decimal null,
	col5 numeric null,
	col6 real null,
	col7 double precision null,

	col10 money null,

	col11 varchar(10) null,
	col12 text null,
	col13 citext null,

	col14 bytea null,

	col15 timestamp without time zone null,
	col16 timestamp with time zone null,
	col17 date null,
	col18 time without time zone null,
	col19 time with time zone null,
	col20 interval null,

	col21 boolean null,

	col22 mood null,

	col23 point null,
	col24 line null,
	col25 lseg null,
	col26 box null,
	col27 path null,
	col28 polygon null,
	col29 circle null,

	col30 cidr null,
	col31 inet null,
	col32 macaddr null,
	col33 macaddr8 null,

	col34 bit(3) null,

	col35 tsvector null,
	col36 tsquery null,

	col37 uuid null,

	col38 xml null,

	col39 json null,
	col40 jsonb null,
	col41 jsonpath null,

	col42 integer[] null,
	col43 text[] null,
	
	col44 complex null,

	col45 int4range null,
	col46 int8range null,
	col47 numrange null,
	col48 tsrange null,
	col49 tstzrange null,
	col50 daterange null

	
);
`)
	if err != nil {
		panic(err)
	}

	_, err = rt.pool.Exec(context.Background(), fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE all_pg_types", rt.testCtx.pgTestOpts.Pub))
	if err != nil {
		panic(err)
	}
}

// We no longer have table auto reload
// we have a writer mode set per table - so we explicitly require the mode to be set for all tables
func DoNotTestNewTableAutoReload(t *testing.T) {
	test := &NewTableAutoReloadTest{}
	test.Setup(t, testOptsNoTxTime, defaultHSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestPubAutoReload(t *testing.T) {
	test := &PubAutoReloadTest{}
	test.Setup(t, testOptsNoTxTime, defaultHSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestNewColumnAutoReload(t *testing.T) {
	test := &NewColumnAutoReloadTest{}
	test.Setup(t, testOptsNoTxTime, defaultHSETCfg, test.updateConfig)
	defer test.TearDown()
	test.runTest(t, test)
}

type newTestTable struct {
	TableID string
}

type NewTableAutoReloadTest struct {
	pg2RedisTest
	ntt *newTestTable
}

func (rt *NewTableAutoReloadTest) arrange() {
	rt.pool.Exec(context.Background(), "DROP TABLE IF EXISTS new_test_table")
}

func (rt *NewTableAutoReloadTest) act() {
	_, err := rt.pool.Exec(context.Background(), "CREATE TABLE new_test_table (tableid text primary key)")
	if err != nil {
		panic(err)
	}

	_, err = rt.pool.Exec(context.Background(), fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE new_test_table", rt.testCtx.pgTestOpts.Pub))
	if err != nil {
		panic(err)
	}

	rt.ntt = &newTestTable{
		TableID: "newid",
	}

	_, err = rt.pool.Exec(context.Background(), "INSERT INTO new_test_table (tableid) VALUES ($1)", rt.ntt.TableID)
	if err != nil {
		panic(err)
	}
}

func (rt *NewTableAutoReloadTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.new_test_table:%s", rt.ntt.TableID)
	res, err := rt.redisClient.HGetAll(context.Background(), entryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, rt.ntt.TableID, res["tableid"])
}

type PubAutoReloadTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *PubAutoReloadTest) act() {
	if rt.pgVersion < pgschema.V15 {
		return
	}

	_, err := rt.pool.Exec(context.Background(),
		fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s (id)", rt.testCtx.pgTestOpts.Pub, rt.testCtx.pgTestOpts.Table))
	if err != nil {
		panic(err)
	}

	rt.tt = &TestTable{
		ID:        200,
		FieldInt:  300,
		FieldData: generateRandomData(5),
	}
	rt.mustInsert(rt.tt)
}

func (rt *PubAutoReloadTest) assert(t *testing.T) {
	if rt.pgVersion < pgschema.V15 {
		return
	}

	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt.ID)
	res, err := rt.redisClient.HGetAll(context.Background(), entryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, res["id"], fmt.Sprintf("%d", rt.tt.ID))
	_, ok := res["field_int"]
	assert.False(t, ok)
	_, ok = res["field_data"]
	assert.False(t, ok)
}

type NewColumnAutoReloadTest struct {
	pg2RedisTest
	tt1 *TestTable

	tt2       *TestTable
	newColumn int
}

func (rt *NewColumnAutoReloadTest) act() {
	// first we insert with old schema
	rt.tt1 = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: generateRandomData(2),
	}
	rt.mustInsert(rt.tt1)

	// add new_column to the table
	_, err := rt.pool.Exec(context.Background(), fmt.Sprintf("ALTER TABLE %s ADD new_column integer null", rt.testCtx.pgTestOpts.Table))
	if err != nil {
		panic(err)
	}

	tt := &TestTable{
		ID:        100500,
		FieldInt:  12,
		FieldData: generateRandomData(10),
	}
	newColumn := 876

	r, err := rt.pool.Query(context.Background(),
		fmt.Sprintf("INSERT INTO %s (id, field_int, field_data, new_column) VALUES ($1, $2, $3, $4) RETURNING txid_current()", rt.testCtx.pgTestOpts.Table),
		tt.ID,
		tt.FieldInt,
		tt.FieldData,
		newColumn)

	if err != nil {
		panic(fmt.Errorf("failed to insert into %s", rt.testCtx.pgTestOpts.Table))
	}

	defer r.Close()
	var xid uint32
	if !r.Next() {
		panic(fmt.Errorf("insert didn't return current xid"))
	}

	err = r.Scan(&xid)
	if err != nil {
		panic("failed to scan xid after insert")
	}

	rt.tt2 = tt
	rt.newColumn = newColumn
}

func (rt *NewColumnAutoReloadTest) assert(t *testing.T) {
	entryKey2 := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt2.ID)

	assertHSetEntry(t, rt.redisClient, rt.tt1, rt.testCtx.pgTestOpts.Table)

	res, err := rt.redisClient.HGetAll(context.Background(), entryKey2).Result()
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%d", rt.newColumn), res["new_column"])
}

func (rt *NewColumnAutoReloadTest) updateConfig(appConf *redisconfig.RedisAppConfig) {
	tableKey := pgschema.MakeTableName("public", rt.testCtx.pgTestOpts.Table)
	appConf.Tables = map[string]*redisconfig.RedisTableConfig{}
	appConf.Tables[tableKey.FullName] = &redisconfig.RedisTableConfig{
		Commands: convertCommands([]redisconfig.RedisCommandConfig{{"HSET", tableKey.FullName + ":{%pk%}", "{pairs:*}"}}),
	}
}

type TestTable struct {
	ID        int
	FieldInt  int
	FieldData []byte
}

func (tt *TestTable) AsMap() *orderedmap.OrderedMap {
	m := newTestOrderedMap("id", "field_int", "field_data")
	m.Set("id", tt.ID)
	m.Set("field_int", tt.FieldInt)
	m.Set("field_data", decode.EncodeBinary(tt.FieldData, decode.BinaryEncodingBase64))

	return m
}

func (tt *TestTable) AsMapString() map[string]any {
	m := make(map[string]any)
	m["id"] = fmt.Sprintf("%d", tt.ID)
	m["field_int"] = fmt.Sprintf("%d", tt.FieldInt)
	m["field_data"] = decode.EncodeBinary(tt.FieldData, decode.BinaryEncodingBase64)

	return m
}

func (tt *TestTable) AsJSON() string {
	m := tt.AsMap()
	j, err := m.MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("failed to marshal as json: %v", err))
	}

	return string(j)
}

type pg2RedisTest struct {
	pgConfig       pgconfig.PgConfig
	redisOpts      *redis.Options
	pool           *pgxpool.Pool
	testCtx        *TestContext
	redisClient    *redis.Client
	listenerConfig RedisListenerOptions
	alterWriter    func(w RedisWriter) RedisWriter
	connector      *pgconnector.PGConnector

	pgVersion pgschema.PGVersion
}

type updateConfFunc func(appConfig *redisconfig.RedisAppConfig)

type RedisTestSetup interface {
	integrationtest.TableNameProvider

	UpdateConfig(appConfig *redisconfig.RedisAppConfig)
}

// MustSetup setups test in a new way. Setup is now obsolete.
func (rt *pg2RedisTest) MustSetup(t *testing.T, test RedisTestSetup, listenerConfig RedisListenerOptions, commands []redisconfig.RedisCommandConfig) {
	appConf, err := redisconfig.LoadAppConfig("")
	if err != nil {
		panic(err)
	}

	pgConfig := appConf.Postgres
	wellFormed := strings.ToLower(strings.Replace(t.Name(), "/", "", -1))
	pgConfig.Repl.Slot = wellFormed + "_slot"
	pgConfig.Repl.Pub = wellFormed + "_pub"
	redisOpts, err := appConf.ReadRedisOptions(3 * time.Second)
	if err != nil {
		panic(fmt.Errorf("failed to read Redis options: %w", err))
	}

	connStr := pgConfig.Conn.AsOpt().CreateConnStr()
	pgPool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		panic(fmt.Errorf("failed to initialize test %s: %v", t.Name(), err))
	}

	listenerConfig.ListenerOpts.Publication = pgConfig.Repl.Pub
	listenerConfig.ListenerOpts.AppName = string(licensemanager.ProductPG2REDIS)
	if listenerConfig.WriterOpts.FlushOpts.Interval == 0 {
		listenerConfig.WriterOpts.FlushOpts.Interval = 500 * time.Microsecond
	}

	rt.pgConfig = pgConfig
	rt.redisOpts = redisOpts
	rt.pool = pgPool
	rt.redisClient = redis.NewClient(redisOpts)
	rt.listenerConfig = listenerConfig

	rt.mustCleanup()

	testOptions := integrationtest.MustCreateMultiTestOptionsWithConfig(context.Background(), rt.pgConfig, test)

	var log *zap.Logger
	if enableLog {
		log, _ = zap.NewDevelopment()
	} else {
		log = zap.NewNop()
	}

	rt.testCtx = &TestContext{
		pgMultiTestOpts: testOptions,
		pgTestOpts: integrationtest.PGTestOptions{
			Table:   "",
			Slot:    testOptions.Slot,
			Pub:     testOptions.Pub,
			Timeout: testOptions.Timeout,
			Opts:    testOptions.Opts,
		},
	}

	for _, tableConfig := range appConf.Tables {
		tableConfig.Commands = convertCommands(commands)
	}

	test.UpdateConfig(appConf)

	cfgBuilder := redisconfig.NewFromConfig(appConf)
	cfgBuilder.WellForm(nil)

	connector, err := pgconnector.New(appConf.Postgres.Conn, log)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	t.Cleanup(func() {
		connector.Close(context.Background())
	})

	ver, err := connector.PrimaryVersion()
	require.NoError(t, err)
	rt.pgVersion = ver
	rt.connector = connector

	redisListener := newListener(rt.redisOpts, listenerConfig, connector, appConf, log, func(w RedisWriter) RedisWriter {
		if rt.alterWriter != nil {
			return rt.alterWriter(w)
		}

		return w
	})

	rt.testCtx.listener = redisListener

	err = redisListener.LoadPubTables(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to read publication tables: %v", err))
	}
	err = redisListener.Ping(context.Background())
	if err != nil {
		panic(fmt.Errorf("unable to connect to redis: %v", err))
	}
}

// Setup setups tests. Obsolete. Use MustSetup.
func (rt *pg2RedisTest) Setup(t *testing.T, listenerConfig RedisListenerOptions, commands []redisconfig.RedisCommandConfig, f ...any) {
	appConf, err := redisconfig.LoadAppConfig("")
	if err != nil {
		panic(err)
	}

	pgConfig := appConf.Postgres
	wellFormed := strings.ToLower(strings.Replace(t.Name(), "/", "", -1))
	pgConfig.Repl.Slot = wellFormed + "_slot"
	pgConfig.Repl.Pub = wellFormed + "_pub"
	redisOpts, err := appConf.ReadRedisOptions(3 * time.Second)
	if err != nil {
		panic(fmt.Errorf("failed to read Redis options: %w", err))
	}

	connStr := pgConfig.Conn.AsOpt().CreateConnStr()
	pgPool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		panic(fmt.Errorf("failed to initialize test %s: %v", t.Name(), err))
	}

	listenerConfig.ListenerOpts.Publication = pgConfig.Repl.Pub
	listenerConfig.ListenerOpts.AppName = string(licensemanager.ProductPG2REDIS)
	if listenerConfig.WriterOpts.FlushOpts.Interval == 0 {
		listenerConfig.WriterOpts.FlushOpts.Interval = 500 * time.Microsecond
	}

	rt.pgConfig = pgConfig
	rt.redisOpts = redisOpts
	rt.pool = pgPool
	rt.redisClient = redis.NewClient(redisOpts)
	rt.listenerConfig = listenerConfig

	rt.mustCleanup()

	testOptions := integrationtest.MustCreateTestOptionsWithConfig(context.Background(), rt.pgConfig)

	var log *zap.Logger
	if enableLog {
		log, _ = zap.NewDevelopment()
	} else {
		log = zap.NewNop()
	}

	rt.testCtx = &TestContext{
		pgTestOpts: testOptions,
	}

	if len(appConf.Tables) == 0 {
		appConf.Tables[fmt.Sprintf("public.%s", testOptions.Table)] = &redisconfig.RedisTableConfig{
			Commands: convertCommands(commands),
		}
	} else {
		for _, tableConfig := range appConf.Tables {
			tableConfig.Commands = convertCommands(commands)
		}
	}

	if len(f) > 0 {
		for _, confFunc := range f {
			if cf, ok := confFunc.(func(*redisconfig.RedisAppConfig)); ok {
				cf(appConf)
			} else if cf, ok := confFunc.(func(*redisconfig.RedisAppConfig, []redisconfig.RedisCommandConfig)); ok {
				cf(appConf, commands)
			} else if cf, ok := confFunc.(updateConfFunc); ok {
				cf(appConf)
			}
		}
	}

	cfgBuilder := redisconfig.NewFromConfig(appConf)
	cfgBuilder.WellForm(nil)

	connector, err := pgconnector.New(appConf.Postgres.Conn, log)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	t.Cleanup(func() {
		connector.Close(context.Background())
	})

	ver, err := connector.PrimaryVersion()
	require.NoError(t, err)
	rt.pgVersion = ver
	rt.connector = connector

	redisListener := newListener(rt.redisOpts, listenerConfig, connector, appConf, log, func(w RedisWriter) RedisWriter {
		if rt.alterWriter != nil {
			return rt.alterWriter(w)
		}

		return w
	})

	rt.testCtx.listener = redisListener

	err = redisListener.LoadPubTables(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to read publication tables: %v", err))
	}
	err = redisListener.Ping(context.Background())
	if err != nil {
		panic("unable to connect to redis")
	}

}

func (rt *pg2RedisTest) mustCleanup() {
	_, err := rt.pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", rt.pgConfig.Repl.Pub))
	if err != nil {
		panic(fmt.Errorf("failed to drop publication %s: %v", rt.pgConfig.Repl.Pub, err))
	}

	_, err = rt.pool.Exec(context.Background(),
		fmt.Sprintf(`select pg_drop_replication_slot($1)
										  	where exists (select * 
										  	              from pg_replication_slots
										  	              where slot_name = $1)`),
		rt.pgConfig.Repl.Slot,
	)
	if err != nil {
		panic(fmt.Errorf("failed to drop replication slot %s: %v", rt.pgConfig.Repl.Slot, err))
	}
}

type TestContext struct {
	pgTestOpts      integrationtest.PGTestOptions
	pgMultiTestOpts integrationtest.PGTestMultiOptions
	listener        *RedisListener
	runCtx          context.Context
}

type TestRunner interface {
	act()
	assert(t *testing.T)
}

type TestArranger interface {
	arrange()
}

func (rt *pg2RedisTest) runTestFromLSN(t *testing.T, runner TestRunner, lsn pglogrepl.LSN) {
	if a, ok := runner.(TestArranger); ok {
		a.arrange()
	}

	var err error

	ctx, cancel := context.WithCancel(context.Background())
	rt.testCtx.runCtx = ctx
	rt.testCtx.listener.Start(ctx)

	repl := replicator.MustCreateWithLogger(replicator.NewOptions(rt.testCtx.pgTestOpts.Slot,
		rt.testCtx.pgTestOpts.Pub,
		500*time.Millisecond,
		1*time.Second,
		1*time.Minute,
		1000),
		rt.testCtx.listener.Listener(),
		pg2stats.NewNop(),
		rt.connector,
		"redis_test_app",
		rt.testCtx.listener.logger)
	doneChan := make(chan error)
	defer close(doneChan)

	if lsn.String() != "0/0" {
		err = repl.StartFromLsn(ctx, lsn, doneChan)
	} else {
		err = repl.Start(ctx, doneChan)
	}

	if err != nil {
		panic(fmt.Errorf("failed to start replication: %w", err))
	}

	runner.act()
	rt.mustSendHeartbeat()
	endXid := rt.mustReadXid()

	// -1 because we need previous transaction
	repl.WatchXID = endXid - 1

	// Redis tests are very fast.
	// Often the eventCh in the redis_writer is not drained.
	// So we make sure that it has a chance to run by calling runtime.Gosched() and sleeping for a bit.
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)

	rt.testCtx.listener.logger.Info("WatchXID", zap.Uint32(pglogger.XIDParam, repl.WatchXID))
	// wait until it quits but no more than one minute
	timeout, cancelT := context.WithTimeout(context.Background(), time.Minute)
	defer cancelT()
	timedOut := false

	select {
	case _ = <-doneChan:
		break
	case _ = <-timeout.Done():
		timedOut = true
	}

	require.False(t, timedOut, "test has timed out")
	t.Log(t.Name(), ":", "graceful shutdown")
	err = repl.GracefulShutdown(context.Background())
	require.NoError(t, err)
	// stop replicator
	cancel()

	runner.assert(t)
}

func (rt *pg2RedisTest) runTest(t *testing.T, runner TestRunner) {
	rt.runTestFromLSN(t, runner, pglogrepl.LSN(0))
}

func (rt *pg2RedisTest) TearDown() {
	defer rt.pool.Close()

	if rt.testCtx.pgTestOpts.Table != "" {
		_, err := rt.pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE %s", rt.testCtx.pgTestOpts.Table))
		if err != nil {
			panic(err)
		}
	}

	if len(rt.testCtx.pgMultiTestOpts.Tables) > 0 {
		for _, table := range rt.testCtx.pgMultiTestOpts.Tables {
			_, err := rt.pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE %s", table))
			if err != nil {
				panic(err)
			}
		}
	}
}

func (rt *pg2RedisTest) mustReadCommitTime(xid uint32) time.Time {
	r, err := rt.pool.Query(context.Background(), "SELECT pg_xact_commit_timestamp($1)", xid)
	if err != nil {
		panic(fmt.Errorf("failed to pg_xact_commit_timestamp for xid %d: %v", xid, err))
	}

	defer r.Close()
	r.Next()
	var commitTime time.Time
	err = r.Scan(&commitTime)
	if err != nil {
		panic(fmt.Errorf("failed to scan result from pg_xact_commit_timestamp for xid %d: %v", xid, err))
	}

	return commitTime
}

func (rt *pg2RedisTest) mustReadLSN() pglogrepl.LSN {
	r, err := rt.pool.Query(context.Background(), "SELECT pg_current_wal_insert_lsn()")
	if err != nil {
		panic(fmt.Errorf("failed to read pg_current_wal_insert_lsn: %v", err))
	}

	defer r.Close()
	r.Next()
	var lsn pglogrepl.LSN
	err = r.Scan(&lsn)
	if err != nil {
		panic(fmt.Errorf("failed to LSN: %v", err))
	}

	return lsn
}

func (rt *pg2RedisTest) mustSendHeartbeat() {
	tx, err := rt.pool.Begin(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to begin transaction: %v", err))
	}

	defer tx.Rollback(context.Background())

	var xid uint32
	err = tx.QueryRow(context.Background(), "select txid_current()").Scan(&xid)
	if err != nil {
		panic(fmt.Errorf("failed to read txid: %v", err))
	}

	var lsn string
	err = tx.QueryRow(context.Background(), "select pg_logical_emit_message(true, $1, $2)::text",
		string(licensemanager.ProductPG2REDIS),
		pg2buffer.MsgHeartbeat,
	).Scan(&lsn)

	if err != nil {
		panic(fmt.Errorf("failed to send heartbeat: %v", err))
	}

	err = tx.Commit(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to commit heartbeat: %v", err))
	}
}

func (rt *pg2RedisTest) mustReadXid() uint32 {
	r, err := rt.pool.Query(context.Background(), "SELECT txid_current()")
	if err != nil {
		panic(fmt.Errorf("failed to read current xid: %v", err))
	}

	defer r.Close()
	r.Next()
	var xid uint32
	err = r.Scan(&xid)
	if err != nil {
		panic(fmt.Errorf("failed to read xid: %v", err))
	}

	return xid
}

func (rt *pg2RedisTest) mustInsert(tt *TestTable) uint32 {
	r, err := rt.pool.Query(context.Background(),
		fmt.Sprintf("INSERT INTO %s (id, field_int, field_data) VALUES ($1, $2, $3) RETURNING txid_current()", rt.testCtx.pgTestOpts.Table),
		tt.ID,
		tt.FieldInt,
		tt.FieldData)

	if err != nil {
		panic(fmt.Errorf("failed to insert into %s", rt.testCtx.pgTestOpts.Table))
	}

	defer r.Close()
	var xid uint32
	if !r.Next() {
		panic(fmt.Errorf("insert didn't return current xid"))
	}

	err = r.Scan(&xid)
	if err != nil {
		panic("failed to scan xid after insert")
	}

	return xid
}

func (rt *pg2RedisTest) mustInsertTx(tt *TestTable, tx pgx.Tx) {
	_, err := tx.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s (id, field_int, field_data) VALUES ($1, $2, $3)", rt.testCtx.pgTestOpts.Table),
		tt.ID,
		tt.FieldInt,
		tt.FieldData)

	if err != nil {
		panic(fmt.Errorf("failed to insert into %s", rt.testCtx.pgTestOpts.Table))
	}
}

func (rt *pg2RedisTest) mustUpdate(tt *TestTable) uint32 {
	r, err := rt.pool.Query(context.Background(),
		fmt.Sprintf("UPDATE %s SET field_int = $1, field_data = $2 WHERE id = $3 RETURNING txid_current()", rt.testCtx.pgTestOpts.Table),
		tt.FieldInt,
		tt.FieldData,
		tt.ID)
	if err != nil {
		panic(fmt.Errorf("failed to update %s: %v", rt.testCtx.pgTestOpts.Table, err))
	}

	defer r.Close()

	r.Next()
	var xid uint32
	err = r.Scan(&xid)
	if err != nil {
		panic(fmt.Errorf("failed to read xid: %v", err))
	}

	return xid
}

func (rt *pg2RedisTest) mustUpdateTx(tt *TestTable, tx pgx.Tx) {
	_, err := tx.Exec(context.Background(),
		fmt.Sprintf("UPDATE %s SET field_int = $1, field_data = $2 WHERE id = $3", rt.testCtx.pgTestOpts.Table),
		tt.FieldInt,
		tt.FieldData,
		tt.ID)

	if err != nil {
		panic(fmt.Errorf("failed to update with tx %s", rt.testCtx.pgTestOpts.Table))
	}
}

func (rt *pg2RedisTest) mustDelete(tt *TestTable) {
	_, err := rt.pool.Exec(context.Background(),
		fmt.Sprintf("DELETE FROM %s WHERE id = $1", rt.testCtx.pgTestOpts.Table),
		tt.ID)

	if err != nil {
		panic(fmt.Errorf("failed to delete %s", rt.testCtx.pgTestOpts.Table))
	}
}

func (rt *pg2RedisTest) assertLSNInRedis(t *testing.T) {
	commitPos := rt.testCtx.listener.listener.CommitPos().LSN
	assert.NotEqual(t, commitPos.String(), "0/0")

	val, err := rt.redisClient.Get(context.Background(), redisLSNKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		panic(err)
	}

	var lsn pglogrepl.LSN
	err = lsn.Scan(val)
	require.NoError(t, err)
	if err == nil {
		assert.Equal(t, commitPos.String(), lsn.String())
	}
}

var (
	enableLog bool
)

func TestMain(m *testing.M) {

	flag.BoolVar(&enableLog, "log", false, "enable log")

	// manually call flag.Parse so that testing.Short() won't panic
	flag.Parse()

	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		os.Exit(m.Run())
	}

	// docker run --name postgres-14 -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres:14
	var pgImages []string
	var redisImages []string
	if testing.Short() {
		pgImages = []string{
			"docker.io/postgres:17",
		}
		redisImages = []string{
			"docker.io/redis:7",
		}
	} else {
		pgImages = integrationtest.PGImages
		redisImages = []string{
			"docker.io/redis:8",
			"docker.io/redis:7",
			"docker.io/redis:6",
		}
	}

	tempDir, err := os.MkdirTemp("", "pg2redis-test-certs*")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tempDir)

	caCrt, caKey := generateCA()
	serverCrt, serverKey := generateServerCert(caCrt, caKey)
	clientCrt, clientKey := generateClientCert(caCrt, caKey)

	serverCertOpts := RedisCertOpts{
		CAFile:   "ca.crt",
		CertFile: "redisserver.crt",
		KeyFile:  "redisserver.key",
		TempDir:  tempDir,
	}

	clientCertOpts := RedisCertOpts{
		CAFile:   "ca.crt",
		CertFile: "redisclient.crt",
		KeyFile:  "redisclient.key",
		TempDir:  tempDir,
	}

	writePEM(tempDir, serverCertOpts.CAFile, "CERTIFICATE", caCrt.Raw)
	writePEM(tempDir, serverCertOpts.CertFile, "CERTIFICATE", serverCrt.Raw)
	writePEM(tempDir, serverCertOpts.KeyFile, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(serverKey))
	writePEM(tempDir, clientCertOpts.CertFile, "CERTIFICATE", clientCrt.Raw)
	writePEM(tempDir, clientCertOpts.KeyFile, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientKey))

	fmt.Printf("Generated certificates and keys in %s\n", tempDir)
	fmt.Printf("CA: %s\n", serverCertOpts.CAFile)
	fmt.Printf("Server: %s\n", serverCertOpts.CertFile)
	fmt.Printf("Client: %s\n", clientCertOpts.CertFile)

	for _, pgImage := range pgImages {
		for _, redisImage := range redisImages {
			fmt.Println("----- TEST START -----")
			fmt.Printf("PG: %v, Redis: %v\n", pgImage, redisImage)

			runMainWithContainers(m, pgImage, redisImage, serverCertOpts, clientCertOpts)
			fmt.Println("----- TEST END   ----- ")
		}
	}
}

type RedisCertOpts struct {
	CAFile   string
	CertFile string
	KeyFile  string
	TempDir  string
}

func writePEM(dir, name, typ string, derBytes []byte) {
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	pem.Encode(f, &pem.Block{Type: typ, Bytes: derBytes})
}

func generateServerCert(caCert *x509.Certificate, caKey *rsa.PrivateKey) (*x509.Certificate, *rsa.PrivateKey) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "redis",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, _ := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	cert, _ := x509.ParseCertificate(certDER)
	return cert, key
}

func generateClientCert(caCert *x509.Certificate, caKey *rsa.PrivateKey) (*x509.Certificate, *rsa.PrivateKey) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "redis-client",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	certDER, _ := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	cert, _ := x509.ParseCertificate(certDER)
	return cert, key
}

func generateCA() (*x509.Certificate, *rsa.PrivateKey) {
	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "redis-test-CA",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, _ := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)
	return caCert, caKey
}

func runMainWithContainers(m *testing.M, pgImage, redisImage string, serverCertOpts RedisCertOpts, clientCertOpts RedisCertOpts) {
	metrics.UnregisterAllMetrics()

	ctx := context.Background()

	redisReq := testcontainers.ContainerRequest{
		Image:        redisImage,
		ExposedPorts: []string{"6379/tcp"},
		Cmd: []string{
			"redis-server",
			"--port", "0",
			"--tls-port", "6379",
			"--tls-cert-file", fmt.Sprintf("/certs/%s", serverCertOpts.CertFile),
			"--tls-key-file", fmt.Sprintf("/certs/%s", serverCertOpts.KeyFile),
			"--tls-ca-cert-file", fmt.Sprintf("/certs/%s", serverCertOpts.CAFile),
			"--tls-auth-clients", "yes",
		},
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(serverCertOpts.TempDir, "/certs"),
		),
		WaitingFor: wait.ForAll(wait.ForLog("* Ready to accept connections"), wait.ForExposedPort()),
	}

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: redisReq,
		Started:          true,
	})

	if err != nil {
		panic(fmt.Errorf("failed to start redis container: %v", err))
	}

	initScripts := []string{filepath.Join("../internal/integrationtest/testdata", "initpg.sql")}
	if !strings.Contains(pgImage, "postgres:12") {
		initScripts = append(initScripts, filepath.Join("../internal/integrationtest/testdata", "initpgv13.sql"))
	}

	pgC, err := integrationtest.CreatePGContainer(ctx, pgImage, redisconfig.Prefix(), pgCont.WithInitScripts(initScripts...))
	if err != nil {
		panic(fmt.Errorf("failed to start postgres container: %v", err))
	}

	defer func() {
		redisC.Terminate(ctx)
		pgC.Terminate(ctx)
	}()

	host, _ := redisC.Host(ctx)
	port, _ := redisC.MappedPort(ctx, "6379/tcp")

	if err != nil {
		fmt.Printf("redis endpoint error: %+v\n", err)
		panic(err)
	}
	fmt.Printf("redis endpoint: %s:%+v\n", host, port)

	os.Setenv("PG2REDIS_REDIS_CONN_HOST", host)
	os.Setenv("PG2REDIS_REDIS_CONN_PORT", port.Port())
	os.Setenv("PG2REDIS_REDIS_CONN_DATABASE", "0")
	os.Setenv("PG2REDIS_REDIS_CONN_TLS_CERT", filepath.Join(clientCertOpts.TempDir, clientCertOpts.CertFile))
	os.Setenv("PG2REDIS_REDIS_CONN_TLS_KEY", filepath.Join(clientCertOpts.TempDir, clientCertOpts.KeyFile))
	os.Setenv("PG2REDIS_REDIS_CONN_TLS_ROOTCERT", filepath.Join(serverCertOpts.TempDir, serverCertOpts.CAFile))

	runMain(m)
}

func runMain(m *testing.M) {
	appConf, err := redisconfig.LoadAppConfig("")
	if err != nil {
		panic("failed to load app config: " + err.Error())
	}

	integrationtest.MustLogicalDecodingWorkMemWithConfig(appConf.Postgres)
	m.Run()
}

func generateRandomData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func convertCommands(cmds []redisconfig.RedisCommandConfig) []any {
	res := make([]any, len(cmds))

	for i, cmd := range cmds {
		res[i] = []string(cmd)
	}

	return res
}
