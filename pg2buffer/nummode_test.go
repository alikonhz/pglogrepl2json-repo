package pg2buffer

import (
	"fmt"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest/testpgreplicator"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	numModesTable    = "test_numbers"
	numModesSlotName = "test_numbers_slot"
	numModesPubName  = "test_numbers_pub"
	createSQL        = `
		DROP TABLE IF EXISTS test_numbers;
		CREATE TABLE test_numbers (
			id              bigint PRIMARY KEY,
			small           smallint,
			normal_int      integer,
			big             bigint,
			real_val        real,
			double_val      double precision,
			numeric_val     numeric(20,10),
			null_int    integer,
			null_num numeric
		);
	`
)

type numModeTestRun struct {
	name         string
	insertSQL    string
	expectedJSON string
}

func TestNumericModesFloat(t *testing.T) {
	runNumModeTest(t, pgconfig.PgNumericModeFloat, []numModeTestRun{
		{
			name:         "normal values 1.23",
			insertSQL:    "(1, 123, 123, 1230, 1.23, 1.23, 1.23, NULL, NULL)",
			expectedJSON: `{"id":1,"small":123,"normal_int":123,"big":1230,"real_val":1.23,"double_val":1.23,"numeric_val":1.2300000000,"null_int":null,"null_num":null}`,
		},
		{
			name:         "min values",
			insertSQL:    "(2, -32768, -2147483648, -9223372036854775808, 1.401298464324817070923729583289916131280e-45, 4.9406564584124654417656879286822137236505980e-324, -9999999999.9999999999, NULL, NULL)",
			expectedJSON: `{"id":2,"small":-32768,"normal_int":-2147483648,"big":-9223372036854775808,"real_val":1e-45,"double_val":5e-324,"numeric_val":-9999999999.9999999999,"null_int":null,"null_num":null}`,
		},
		{
			name:         "max values",
			insertSQL:    "(3, 32767, 2147483647, 9223372036854775807, 3.40282346638528859811704183484516925440e+38, 1.79769313486231570814527423731704356798070e+308, 9999999999.9999999999, NULL, NULL)",
			expectedJSON: `{"id":3,"small":32767,"normal_int":2147483647,"big":9223372036854775807,"real_val":3.4028235e+38,"double_val":1.7976931348623157e+308,"numeric_val":9999999999.9999999999,"null_int":null,"null_num":null}`,
		},
		{
			name:         "zero values",
			insertSQL:    "(4, 0, 0, 0, 0.0, 0.0, 0.0, NULL, NULL)",
			expectedJSON: `{"id":4,"small":0,"normal_int":0,"big":0,"real_val":0,"double_val":0,"numeric_val":0.0000000000,"null_int":null,"null_num":null}`,
		},
		{
			name:         "negative values",
			insertSQL:    "(5, -1, -100, -1000, -1.23, -1.23456789, -123.4567890123, NULL, NULL)",
			expectedJSON: `{"id":5,"small":-1,"normal_int":-100,"big":-1000,"real_val":-1.23,"double_val":-1.23456789,"numeric_val":-123.4567890123,"null_int":null,"null_num":null}`,
		},
		{
			name:         "special values - NaN",
			insertSQL:    "(7, NULL, NULL, NULL, 'NaN', 'NaN', 'NaN', NULL, NULL)", // in float mode we convert NaN to null
			expectedJSON: `{"id":7,"small":null,"normal_int":null,"big":null,"real_val":null,"double_val":null,"numeric_val":null,"null_int":null,"null_num":null}`,
		},
		{
			name:         "special values - +Infinity",
			insertSQL:    "(8, NULL, NULL, NULL, 'Infinity', 'Infinity', NULL, NULL, NULL)", // in float mode we replace Infinity with Max float32/64
			expectedJSON: `{"id":8,"small":null,"normal_int":null,"big":null,"real_val":3.4028234663852886e+38,"double_val":1.7976931348623157e+308,"numeric_val":null,"null_int":null,"null_num":null}`,
		},
		{
			name:         "special values - -Infinity",
			insertSQL:    "(8, NULL, NULL, NULL, '-Infinity', '-Infinity', NULL, NULL, NULL)", // in float mode we replace -Infinity with Min float32/64
			expectedJSON: `{"id":8,"small":null,"normal_int":null,"big":null,"real_val":1.401298464324817e-45,"double_val":5e-324,"numeric_val":null,"null_int":null,"null_num":null}`,
		},
	})
}

func TestNumericModesString(t *testing.T) {
	runNumModeTest(t, pgconfig.PgNumericModeString, []numModeTestRun{
		{
			name:         "normal values 1.23",
			insertSQL:    "(1, 123, 123, 1230, 1.23, 1.23, 1.23, NULL, NULL)",
			expectedJSON: `{"id":1,"small":123,"normal_int":123,"big":1230,"real_val":"1.23","double_val":"1.23","numeric_val":"1.2300000000","null_int":null,"null_num":null}`,
		},
		{
			name:         "min values",
			insertSQL:    "(2, -32768, -2147483648, -9223372036854775808, 1.401298464324817070923729583289916131280e-45, 4.9406564584124654417656879286822137236505980e-324, -9999999999.9999999999, NULL, NULL)",
			expectedJSON: `{"id":2,"small":-32768,"normal_int":-2147483648,"big":-9223372036854775808,"real_val":"1e-45","double_val":"5e-324","numeric_val":"-9999999999.9999999999","null_int":null,"null_num":null}`,
		},
		{
			name:         "max values",
			insertSQL:    "(3, 32767, 2147483647, 9223372036854775807, 3.40282346638528859811704183484516925440e+38, 1.79769313486231570814527423731704356798070e+308, 9999999999.9999999999, NULL, NULL)",
			expectedJSON: `{"id":3,"small":32767,"normal_int":2147483647,"big":9223372036854775807,"real_val":"3.4028235e+38","double_val":"1.7976931348623157e+308","numeric_val":"9999999999.9999999999","null_int":null,"null_num":null}`,
		},
		{
			name:         "zero values",
			insertSQL:    "(4, 0, 0, 0, 0.0, 0.0, 0.0, NULL, NULL)",
			expectedJSON: `{"id":4,"small":0,"normal_int":0,"big":0,"real_val":"0","double_val":"0","numeric_val":"0.0000000000","null_int":null,"null_num":null}`,
		},
		{
			name:         "negative values",
			insertSQL:    "(5, -1, -100, -1000, -1.23, -1.23456789, -123.4567890123, NULL, NULL)",
			expectedJSON: `{"id":5,"small":-1,"normal_int":-100,"big":-1000,"real_val":"-1.23","double_val":"-1.23456789","numeric_val":"-123.4567890123","null_int":null,"null_num":null}`,
		},
		{
			name:         "special values - NaN",
			insertSQL:    "(7, NULL, NULL, NULL, 'NaN', 'NaN', 'NaN', NULL, NULL)", // in string mode we use NaN
			expectedJSON: `{"id":7,"small":null,"normal_int":null,"big":null,"real_val":"NaN","double_val":"NaN","numeric_val":"NaN","null_int":null,"null_num":null}`,
		},
		{
			name:         "special values - +Infinity",
			insertSQL:    "(8, NULL, NULL, NULL, 'Infinity', 'Infinity', NULL, NULL, NULL)", // in string mode we use Infinity
			expectedJSON: `{"id":8,"small":null,"normal_int":null,"big":null,"real_val":"Infinity","double_val":"Infinity","numeric_val":null,"null_int":null,"null_num":null}`,
		},
		{
			name:         "special values - -Infinity",
			insertSQL:    "(8, NULL, NULL, NULL, '-Infinity', '-Infinity', NULL, NULL, NULL)", // in string mode we use -Infinity
			expectedJSON: `{"id":8,"small":null,"normal_int":null,"big":null,"real_val":"-Infinity","double_val":"-Infinity","numeric_val":null,"null_int":null,"null_num":null}`,
		},
	})
}

func runNumModeTest(t *testing.T, numMode pgconfig.PgNumericMode, testRuns []numModeTestRun) {
	for _, run := range testRuns {
		t.Run(string(numMode)+"_"+run.name, func(t *testing.T) {
			ctx := t.Context()
			log := zap.NewNop()
			//log, _ := zap.NewDevelopment()

			writes := make([]*pgwal.WriteRequest, 0)
			numOfWritesChan := make(chan struct{})
			expectedNumOfWrites := 1

			var onWrite writeFunc = func(req *pgwal.WriteRequest) {
				writes = append(writes, req)
				if len(writes) == expectedNumOfWrites {
					numOfWritesChan <- struct{}{}
				}
			}

			writer := &testPGWALWriter{onWrite: onWrite}

			res, bufListener := mustCreateAndPrepareBufferedListener(t, writer, testpgreplicator.TestPGReplicatorOptions{
				Logger:      log,
				RetryConfig: circuitbreaker.DefaultRetryConfig(),
				Postgres: testpgreplicator.PGOptions{
					Image:          pgImage,
					User:           userName,
					Password:       password,
					CreateTableSQL: createSQL,
					TableName:      numModesTable,
					SlotName:       numModesSlotName,
					PubName:        numModesPubName,
					NumMode:        convertNumericMode(numMode),
				},
			})

			bufListener.Start(ctx)

			repl := replicator.MustCreateWithLogger(replicator.NewOptions(res.Options.Postgres.SlotName,
				res.Options.Postgres.PubName, 5*time.Second, 15*time.Second, 1*time.Hour, 10000),
				bufListener,
				bufListener,
				res.Connector,
				"pg2buffer_nummodes_test",
				log)

			t.Log("starting replicator")

			doneChan := make(chan error)
			err := repl.StartFromLsn(ctx, pglogrepl.LSN(0), doneChan)

			if err != nil {
				t.Fatal("failed to start replicator (#1): " + err.Error())
			}

			_, err = res.Pool.Exec(t.Context(),
				fmt.Sprintf("INSERT INTO %s (id, small, normal_int, big, real_val, double_val, numeric_val, null_int, null_num) VALUES %s",
					numModesTable,
					run.insertSQL))
			require.NoError(t, err)

			t.Log("waiting for writes to finish (#1)")

			timedOut := within(numOfWritesChan)
			if timedOut {
				t.Fatal("timed out waiting for writes to be processed (#1)")
			}

			assertNumModeTest(t, run.expectedJSON, writes)
		})
	}
}

func assertNumModeTest(t *testing.T, expectedJSON string, actual []*pgwal.WriteRequest) {
	assert.Len(t, actual, 1)
	assert.Len(t, actual[0].Entries, 1)

	actualEntry := actual[0].Entries[0]

	j, err := actualEntry.Tuple.MarshalJSON()
	require.NoError(t, err)

	assert.Equal(t, expectedJSON, string(j))
}

func mustCreateAndPrepareBufferedListener(t *testing.T,
	writer PGWALWriter,
	opts testpgreplicator.TestPGReplicatorOptions) (*testpgreplicator.CreateResult, *PGBufferedListener) {
	res := testpgreplicator.MustPrepareForPGReplicator(t, opts)

	tableNameKey := "public." + opts.Postgres.TableName
	bufListener := NewListener(writer, ListenerOptions{
		Publication: opts.Postgres.PubName,
		NumericMode: opts.Postgres.NumMode,
	}, res.Connector,
		map[string]*config.TableConfig{
			tableNameKey: &config.TableConfig{
				Columns: []string{"all"},
				Options: make(map[string]string),
			},
		},
		opts.RetryConfig,
		opts.Logger)

	err := bufListener.LoadPubTables(t.Context())
	require.NoError(t, err)

	return res, bufListener
}
