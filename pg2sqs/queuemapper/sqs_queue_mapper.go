package queuemapper

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsconfig"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/aws/aws-sdk-go-v2/aws"
	"iter"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	SkipQueue = "skip"
)

var (
	groupIDRegExp               = regexp.MustCompile(`\$\{([^}]+)\}`)
	ErrInvalidGroupIDExpression = errors.New("invalid groupID expression for table")
)

type TableQueueConfig struct {
	tableName      string
	queueName      string
	queueURL       string
	fifo           bool
	skip           bool
	needsOpInJSON  bool
	groupIDPattern string
}

func (qc *TableQueueConfig) QueueName() string {
	return qc.queueName
}

func (qc *TableQueueConfig) TableName() string {
	return qc.tableName
}

func (qc *TableQueueConfig) NeedsOpInJSON() bool {
	return qc.needsOpInJSON
}

func (qc *TableQueueConfig) GetDeduplicationID(entry *pgwal.WriteEntry, xid uint32) *string {
	if !qc.fifo {
		return nil
	}

	dedupID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s:%d:%d", entry.Table.FullName, entry.PK, entry.Kind, xid)))

	dedupID = " " + dedupID

	return aws.String(wellFormAWSID(dedupID))
}

func (qc *TableQueueConfig) ValidateMessageGroupID(columns []string) error {
	if qc.groupIDPattern == "" {
		return nil
	}

	allMatches := groupIDRegExp.FindAllString(qc.groupIDPattern, -1)

	var err error

	for _, match := range allMatches {
		macro := match[2 : len(match)-1]
		switch macro {
		case schemaMacro:
		case tableMacro:
		case xidMacro:
			continue
		default:
			if !slices.ContainsFunc(columns, func(col string) bool {
				return strings.EqualFold(macro, col)
			}) {
				err = errors.Join(err, fmt.Errorf("%w: expression %s is not a valid column", ErrInvalidGroupIDExpression, macro))
			}
		}
	}

	return err
}

const (
	schemaMacro = "%schema%"
	tableMacro  = "%table%"
	xidMacro    = "%xid%"
)

func wellFormAWSID(awsID string) string {
	if awsID == "" {
		return ""
	}

	const (
		maxGroupIDLen = 128
		minCode       = 33
		maxCode       = 126
	)
	// Valid values: alphanumeric characters and punctuation !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
	// i.e. ASCII codes >= 33 and <= 126
	// code 33 is !
	// code 126 is ~

	var (
		result   = make([]rune, maxGroupIDLen)
		curIndex = 0
	)

	for _, char := range awsID {
		if curIndex >= maxGroupIDLen {
			break
		}

		if char >= minCode && char <= maxCode {
			result[curIndex] = char
			curIndex++
		}
	}

	if len(result) == 0 {
		return ""
	}

	return string(result[:curIndex])
}

func (qc *TableQueueConfig) GetMessageGroupID(entry *pgwal.WriteEntry, xid uint32) *string {
	if qc.groupIDPattern == "" {
		return nil
	}

	res := groupIDRegExp.ReplaceAllStringFunc(qc.groupIDPattern, func(match string) string {
		// Extract the macro name (remove ${ and })
		macro := match[2 : len(match)-1]
		if macro == schemaMacro {
			return entry.Table.Schema
		}

		if macro == tableMacro {
			return entry.Table.Name
		}

		if macro == xidMacro {
			return strconv.FormatUint(uint64(xid), 10)
		}

		if value, exists := entry.Tuple.Get(macro); exists {
			if value == nil {
				return ""
			}

			return wellFormAWSID(fmt.Sprintf("%v", value))
		}

		return wellFormAWSID(match)
	})

	return aws.String(res)
}

func (qc *TableQueueConfig) Update(queueURL string, fifo bool) {
	qc.queueURL = queueURL
	qc.fifo = fifo
}

func (qc *TableQueueConfig) QueueURL() string {
	return qc.queueURL
}

type SQSQueueMapper struct {
	tableQueueMap map[string]*TableQueueConfig
}

func NewSQSQueueMapper(tableConfigMap map[string]*sqsconfig.SQSTableConfig) *SQSQueueMapper {
	queueMapper := &SQSQueueMapper{
		tableQueueMap: make(map[string]*TableQueueConfig),
	}

	for tableName, tableConfig := range tableConfigMap {
		insertKey, insertConfig := newQueueConfig(pgwal.Insert, tableName, tableConfig.QueueConfig, tableConfig.QueueConfig.Insert)
		updateKey, updateConfig := newQueueConfig(pgwal.Update, tableName, tableConfig.QueueConfig, tableConfig.QueueConfig.Update)
		deleteKey, deleteConfig := newQueueConfig(pgwal.Delete, tableName, tableConfig.QueueConfig, tableConfig.QueueConfig.Delete)

		queueMapper.tableQueueMap[insertKey] = insertConfig
		queueMapper.tableQueueMap[updateKey] = updateConfig
		queueMapper.tableQueueMap[deleteKey] = deleteConfig
	}

	return queueMapper
}

func (qc *SQSQueueMapper) GetQueueConfig(entry *pgwal.WriteEntry) *TableQueueConfig {
	queueKey := makeQueueConfigKey(entry.Table.FullName, entry.Kind)
	queueConfig := qc.tableQueueMap[queueKey]

	return queueConfig
}

func (qc *SQSQueueMapper) AllConfigs() iter.Seq[*TableQueueConfig] {
	return maps.Values(qc.tableQueueMap)
}

func (qc *SQSQueueMapper) GetAllQueueNames() []string {
	queueNamesMap := make(map[string]struct{})

	for tc := range qc.AllConfigs() {
		queueNamesMap[tc.QueueName()] = struct{}{}
	}

	queueNames := slices.Collect(maps.Keys(queueNamesMap))
	slices.Sort(queueNames)

	return queueNames
}

func makeQueueConfigKey(tableName string, opKind pgwal.OpKind) string {
	return fmt.Sprintf("%s-%d", tableName, opKind)
}

func newQueueConfig(opKind pgwal.OpKind, tableName string, defaultConfig, mergeConfig *sqsconfig.SQSQueueConfig) (string, *TableQueueConfig) {
	// by default, we need operation kind (insert, update, delete) in the JSON
	// only if there's a mergeConfig we don't need it
	qc := &TableQueueConfig{
		tableName:      tableName,
		queueName:      defaultConfig.Name,
		queueURL:       defaultConfig.URL,
		fifo:           false,
		needsOpInJSON:  true,
		groupIDPattern: defaultConfig.GroupID,
		skip:           false,
	}

	if mergeConfig != nil {
		qc.needsOpInJSON = false
		if !strings.EqualFold(mergeConfig.Name, SkipQueue) {
			qc.queueName = mergeConfig.Name
			qc.queueURL = mergeConfig.URL
		} else {
			qc.skip = true
			qc.queueName = ""
			qc.queueURL = ""
		}

		if mergeConfig.GroupID != "" {
			qc.groupIDPattern = mergeConfig.GroupID
		}
	}

	if qc.queueName == "" && !qc.skip {
		qc.queueName = strings.ReplaceAll(qc.tableName, ".", "_")
	}

	return makeQueueConfigKey(tableName, opKind), qc
}
