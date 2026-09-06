package pg2redis

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

// let's skip this test for now
// we have numeric mode tests in pg2buffer package
// and in Redis all is converted to string anyway
// TODO: might need to return to it later in the future
func DoNotTestNumericModeFloat(t *testing.T) {
	// Infinity/-Infinity are supported only on PG >= 14
	runs := []struct {
		name         string
		tt           *NumericTestTable
		minPGVersion pgschema.PGVersion
	}{
		{name: "AllValid", tt: &NumericTestTable{
			col4:  newTestValPgNumeric("3.330000", 3.33),
			col5:  newTestValPgNumeric("5.550000", 5.55),
			col6:  newTestValFloat("7.7", 7.7),
			col7:  newTestValFloat("8.88", 8.88),
			col10: newTestValFloat("9.9", 9.9),
		}, minPGVersion: pgschema.V12},
		{name: "Infinity", tt: &NumericTestTable{
			col4:  newTestValPgNumeric("Infinity", math.MaxFloat64),
			col5:  newTestValPgNumeric("Infinity", math.MaxFloat64),
			col6:  newTestValFloat("Infinity", math.MaxFloat64),
			col7:  newTestValFloat("Infinity", math.MaxFloat64),
			col10: newTestValStringExp("99999999", "$99,999,999.00"),
		}, minPGVersion: pgschema.V14},
		{name: "InfinityNeg", tt: &NumericTestTable{
			col4:  newTestValPgNumeric("-Infinity", math.SmallestNonzeroFloat64),
			col5:  newTestValPgNumeric("-Infinity", math.SmallestNonzeroFloat64),
			col6:  newTestValFloat("-Infinity", math.SmallestNonzeroFloat64),
			col7:  newTestValFloat("-Infinity", math.SmallestNonzeroFloat64),
			col10: newTestValStringExp("-99999999", "-$99,999,999.00"),
		}, minPGVersion: pgschema.V14},
		{name: "NaN", tt: &NumericTestTable{
			col4:  newTestValNil("NaN"),
			col5:  newTestValNil("NaN"),
			col6:  newTestValNil("NaN"),
			col7:  newTestValNil("NaN"),
			col10: newTestValStringExp("88.88", "$88.88"),
		}, minPGVersion: pgschema.V12},
	}

	runNumericModeTests(t, decode.NumericEncodingFloat, runs)
}

func TestNumericModeString(t *testing.T) {
	runs := []struct {
		name         string
		tt           *NumericTestTable
		minPGVersion pgschema.PGVersion
	}{
		{name: "AllValid", tt: &NumericTestTable{
			col4:  newTestValString("3.330000"),
			col5:  newTestValString("5.550000"),
			col6:  newTestValString("7.7"),
			col7:  newTestValString("8.88"),
			col10: newTestValStringExp("9.9", "$9.90"),
		}, minPGVersion: pgschema.V12},
		{name: "Infinity", tt: &NumericTestTable{
			col4:  newTestValString("Infinity"),
			col5:  newTestValString("Infinity"),
			col6:  newTestValString("Infinity"),
			col7:  newTestValString("Infinity"),
			col10: newTestValStringExp("99999999", "$99,999,999.00"),
		}, minPGVersion: pgschema.V14},
		{name: "InfinityNeg", tt: &NumericTestTable{
			col4:  newTestValString("-Infinity"),
			col5:  newTestValString("-Infinity"),
			col6:  newTestValString("-Infinity"),
			col7:  newTestValString("-Infinity"),
			col10: newTestValStringExp("-99999999", "-$99,999,999.00"),
		}, minPGVersion: pgschema.V14},
		{name: "NaN", tt: &NumericTestTable{
			col4:  newTestValString("NaN"),
			col5:  newTestValString("NaN"),
			col6:  newTestValString("NaN"),
			col7:  newTestValString("NaN"),
			col10: newTestValStringExp("88.88", "$88.88"),
		}, minPGVersion: pgschema.V12},
	}
	runNumericModeTests(t, decode.NumericEncodingString, runs)
}

func runNumericModeTests(t *testing.T, numericMode decode.NumericEncoding, runs []struct {
	name         string
	tt           *NumericTestTable
	minPGVersion pgschema.PGVersion
}) {
	for _, run := range runs {
		t.Run(run.name, func(t *testing.T) {
			test := &RedisNumericModeStringTest{}
			test.alterWriter = func(w RedisWriter) RedisWriter {
				rw, ok := w.(*redisCircuitBreakerWriter)
				if !ok {
					panic("invalid redis writer (expected circuit breaker)")
				}

				rsw := &redisSliceWriter{
					redisWriter: rw.redisWriter,
					writes:      make([]*pgwal.WriteRequest, 0),
				}

				test.rsw = rsw
				return rsw
			}

			test.tt = run.tt
			test.SetupNumericModeTest(t, numericMode)
			defer test.TearDownNumericModeTest()

			if test.pgVersion >= run.minPGVersion {
				test.runTest(t, test)
			} else {
				t.Skip(fmt.Sprintf("skipping test %s because of PG version", run.name))
			}
		})
	}
}

type numericTestValue struct {
	db       string
	expected any
}

func newTestValString(db string) numericTestValue {
	expected := db
	return numericTestValue{db: db, expected: &expected}
}

func newTestValStringExp(db string, expected string) numericTestValue {
	return numericTestValue{db: db, expected: &expected}
}

func newTestValFloat(db string, expected float64) numericTestValue {
	return numericTestValue{db: db, expected: expected}
}

func newTestValPgNumeric(db string, expected float64) numericTestValue {
	var num pgtype.Numeric
	err := num.Scan(fmt.Sprintf("%v", expected))
	if err != nil {
		panic(err)
	}

	return numericTestValue{db: db, expected: &num}
}

func newTestValNil(db string) numericTestValue {
	return numericTestValue{db: db, expected: nil}
}

type NumericTestTable struct {
	col4  numericTestValue // decimal
	col5  numericTestValue // numeric
	col6  numericTestValue // real
	col7  numericTestValue // double precision
	col10 numericTestValue // money
}

type RedisNumericModeStringTest struct {
	pg2RedisTest
	rsw *redisSliceWriter
	tt  *NumericTestTable
}

func (rt *RedisNumericModeStringTest) act() {
	args := makeArgsForPGVersion(rt.pgVersion, rt.tt.col4.db, rt.tt.col5.db, rt.tt.col6.db, rt.tt.col7.db, rt.tt.col10.db)
	_, err := rt.pool.Exec(context.Background(), `insert into numeric_test (col4, col5, col6, col7, col10) 
values ($1, $2, $3, $4, $5)`, args...)
	if err != nil {
		panic(err)
	}
}

func (rt *RedisNumericModeStringTest) assert(t *testing.T) {
	assert.NotEmpty(t, rt.rsw.notEmptyWrites())
	assert.Len(t, rt.rsw.notEmptyWrites(), 1) // one insert, one heartbeat msg and commit of empty tx
	tuple := rt.rsw.notEmptyWrites()[0].Entries[0].Tuple

	if rt.listenerConfig.ListenerOpts.NumericMode == decode.NumericEncodingString {
		assertNumericTestValueString(t, rt.tt.col4, tuple.GetValue("col4"))
		assertNumericTestValueString(t, rt.tt.col5, tuple.GetValue("col5"))
		assertNumericTestValueString(t, rt.tt.col6, tuple.GetValue("col6"))
		assertNumericTestValueString(t, rt.tt.col7, tuple.GetValue("col7"))
		assertNumericTestValueString(t, rt.tt.col10, tuple.GetValue("col10"))
	} else {
		assert.Equal(t, rt.tt.col4.expected, tuple.GetValue("col4"))
	}
}

func assertNumericTestValueString(t *testing.T, tt numericTestValue, dbVal any) {
	if tt.expected == nil {
		assert.Nil(t, dbVal)
	} else {
		s, ok := tt.expected.(*string)
		if !ok {
			panic("expected *string")
		}

		assert.Equal(t, *s, dbVal)
	}
}

func (rt *RedisNumericModeStringTest) SetupNumericModeTest(t *testing.T, numericMode decode.NumericEncoding) {
	rt.Setup(t, RedisListenerOptions{
		ListenerOpts: pg2buffer.ListenerOptions{
			NumericMode: numericMode,
		},
		WriterOpts: RedisWriterOptions{
			TxTimeOpts: appconfig.NewTxCommitTimeOptions("lastupdated"),
		},
	}, []redisconfig.RedisCommandConfig{{"HSET", pgschema.MakeRowKey("public.testnumericencodingstring_slot_table", "%pk%"), "{pairs:*}"}}, rt.updateConfig, rt.createTables)
}

func (rt *RedisNumericModeStringTest) updateConfig(appConf *redisconfig.RedisAppConfig) {
	appConf.Postgres.NumericMode = pgconfig.PgNumericModeString
}

func (rt *RedisNumericModeStringTest) createTables(appConf *redisconfig.RedisAppConfig) {
	_, err := rt.pool.Exec(context.Background(), `create table numeric_test 
(
	id bigserial not null primary key,
	col4 decimal null,
	col5 numeric null,
	col6 real null,
	col7 double precision null,

	col10 money null
)`)

	if err != nil {
		panic(err)
	}

	_, err = rt.pool.Exec(context.Background(), fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE numeric_test", rt.testCtx.pgTestOpts.Pub))
	if err != nil {
		panic(err)
	}
}

func (rt *RedisNumericModeStringTest) TearDownNumericModeTest() {
	rt.pool.Exec(context.Background(), `drop table numeric_test`)
	defer rt.TearDown()
}

func makeArgsForPGVersion(version pgschema.PGVersion, args ...string) []any {
	res := make([]any, len(args))

	for i := 0; i < len(args); i++ {
		if version >= pgschema.V14 || (!strings.EqualFold("infinity", args[i]) && (!strings.EqualFold("-infinity", args[i]))) {
			res[i] = args[i]
		} else {
			res[i] = "NaN"
		}
	}

	return res
}
