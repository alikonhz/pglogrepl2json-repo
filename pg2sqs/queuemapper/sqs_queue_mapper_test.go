package queuemapper

import (
	"fmt"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

var (
	tableConfig = &pgwal.PGTableWithConfig{
		Cfg:   nil,
		Table: pgschema.NewTable("public", "orders"),
	}
)

func TestDefaultQueueName(t *testing.T) {
	const queueName = "default"
	cfgMap := map[string]*sqsconfig.SQSTableConfig{
		"public.orders": &sqsconfig.SQSTableConfig{ //nolint:exhaustruct
			QueueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: queueName,
			},
		},
	}

	qm := NewSQSQueueMapper(cfgMap)

	testEntries := []pgwal.OpKind{
		pgwal.Insert,
		pgwal.Update,
		pgwal.Delete,
	}

	for _, testEntry := range testEntries {
		t.Run(fmt.Sprintf("%d", testEntry), func(t *testing.T) {
			qc := qm.GetQueueConfig(&pgwal.WriteEntry{ //nolint:exhaustruct
				Table: tableConfig.Table.Name,
				Kind:  testEntry,
			})

			require.NotNil(t, qc)
			assert.Equal(t, queueName, qc.queueName)
		})
	}
}

func TestCustomQueueName(t *testing.T) {
	configMap := map[string]*sqsconfig.SQSTableConfig{
		"public.orders": &sqsconfig.SQSTableConfig{ //nolint:exhaustruct
			QueueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Insert: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderCreated",
				},
				Update: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderUpdated",
				},
				Delete: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderDeleted",
				},
			},
		},
	}

	qm := NewSQSQueueMapper(configMap)

	testEntries := []struct {
		name         string
		kind         pgwal.OpKind
		expectedName string
	}{
		{name: "insert", kind: pgwal.Insert, expectedName: "orderCreated"},
		{name: "update", kind: pgwal.Update, expectedName: "orderUpdated"},
		{name: "delete", kind: pgwal.Delete, expectedName: "orderDeleted"},
	}

	for _, testEntry := range testEntries {
		t.Run(testEntry.name, func(t *testing.T) {
			qc := qm.GetQueueConfig(&pgwal.WriteEntry{ //nolint:exhaustruct
				Kind:  testEntry.kind,
				Table: tableConfig.Table.Name,
			})

			require.NotNil(t, qc)
			assert.Equal(t, testEntry.expectedName, qc.queueName)
		})
	}
}

func TestCustomOp(t *testing.T) {
	const defaultQueueName = "orders"
	testEntries := []struct {
		name          string
		queueConfig   *sqsconfig.SQSQueueConfig
		expectedNames map[pgwal.OpKind]string
	}{
		{
			name: "CustomInsert",
			queueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: defaultQueueName,
				Insert: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderCreated",
				},
			},
			expectedNames: map[pgwal.OpKind]string{
				pgwal.Insert: "orderCreated",
				pgwal.Update: "orders",
				pgwal.Delete: "orders",
			},
		},
		{
			name: "CustomInsertUpdate",
			queueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: defaultQueueName,
				Insert: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderCreated",
				},
				Update: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderUpdated",
				},
			},
			expectedNames: map[pgwal.OpKind]string{
				pgwal.Insert: "orderCreated",
				pgwal.Update: "orderUpdated",
				pgwal.Delete: "orders",
			},
		},
		{
			name: "CustomUpdate",
			queueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: defaultQueueName,
				Update: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderUpdated",
				},
			},
			expectedNames: map[pgwal.OpKind]string{
				pgwal.Insert: "orders",
				pgwal.Update: "orderUpdated",
				pgwal.Delete: "orders",
			},
		},
		{
			name: "CustomDelete",
			queueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: defaultQueueName,
				Delete: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderDeleted",
				},
			},
			expectedNames: map[pgwal.OpKind]string{
				pgwal.Insert: "orders",
				pgwal.Update: "orders",
				pgwal.Delete: "orderDeleted",
			},
		},
		{
			name: "SkipInsert",
			queueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: defaultQueueName,
				Insert: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: SkipQueue,
				},
			},
			expectedNames: map[pgwal.OpKind]string{
				pgwal.Insert: SkipQueue,
				pgwal.Update: "orders",
				pgwal.Delete: "orders",
			},
		},
		{
			name: "CustomInsertUpdateSkipDelete",
			queueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: defaultQueueName,
				Insert: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderCreated",
				},
				Update: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: "orderUpdated",
				},
				Delete: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
					Name: SkipQueue,
				},
			},
			expectedNames: map[pgwal.OpKind]string{
				pgwal.Insert: "orderCreated",
				pgwal.Update: "orderUpdated",
				pgwal.Delete: SkipQueue,
			},
		},
	}

	for _, testEntry := range testEntries {
		t.Run(testEntry.name, func(t *testing.T) {
			cfgMap := map[string]*sqsconfig.SQSTableConfig{
				"public.orders": &sqsconfig.SQSTableConfig{ //nolint:exhaustruct
					QueueConfig: testEntry.queueConfig,
				},
			}

			kinds := []pgwal.OpKind{
				pgwal.Insert,
				pgwal.Update,
				pgwal.Delete,
			}

			for _, kind := range kinds {
				t.Run(fmt.Sprintf("%d", kind), func(t *testing.T) {
					qm := NewSQSQueueMapper(cfgMap)
					qc := qm.GetQueueConfig(&pgwal.WriteEntry{ //nolint:exhaustruct
						Kind:  kind,
						Table: tableConfig.Table.Name,
					})

					require.NotNil(t, qc)

					expectedName := testEntry.expectedNames[kind]
					if strings.EqualFold(expectedName, SkipQueue) {
						assert.Equal(t, "", qc.queueName)
						assert.True(t, qc.skip)
					} else {
						assert.Equal(t, expectedName, qc.queueName)
						assert.False(t, qc.skip)
					}
				})
			}
		})
	}
}

func TestConfigNeedsOpInJSON(t *testing.T) {
	configMap := map[string]*sqsconfig.SQSTableConfig{
		"public.orders": &sqsconfig.SQSTableConfig{ //nolint:exhaustruct
			QueueConfig: &sqsconfig.SQSQueueConfig{ //nolint:exhaustruct
				Name: "orders",
			},
		},
	}

	qm := NewSQSQueueMapper(configMap)

	qc := qm.GetQueueConfig(&pgwal.WriteEntry{ //nolint:exhaustruct
		Kind:  pgwal.Insert,
		Table: tableConfig.Table.Name,
	})

	require.NotNil(t, qc)
	assert.True(t, qc.needsOpInJSON)

	qc = qm.GetQueueConfig(&pgwal.WriteEntry{ //nolint:exhaustruct
		Kind:  pgwal.Update,
		Table: tableConfig.Table.Name,
	})

	require.NotNil(t, qc)
	assert.True(t, qc.needsOpInJSON)

	qc = qm.GetQueueConfig(&pgwal.WriteEntry{ //nolint:exhaustruct
		Kind:  pgwal.Delete,
		Table: tableConfig.Table.Name,
	})

	require.NotNil(t, qc)
	assert.True(t, qc.needsOpInJSON)
}
