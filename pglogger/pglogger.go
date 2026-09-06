package pglogger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	XIDParam     = "xid"
	LSNParam     = "lsn"
	LSN64Param   = "lsn64"
	TableParam   = "table"
	PKParam      = "pk"
	KindParam    = "kind"
	PubNameParam = "pub_name"
	ColumnParam  = "column"
)

func CreateLogger(debug bool, _ string) (*zap.Logger, error) {
	var cfg zap.Config
	if debug {
		cfg = zap.NewDevelopmentConfig()
		cfg.Encoding = "console"
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		cfg.DisableCaller = true
	} else {
		cfg = zap.NewProductionConfig()
		cfg.DisableCaller = true
		cfg.DisableStacktrace = true
	}

	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder

	logger, err := cfg.Build()
	if err != nil {
		return nil, err //nolint
	}

	return logger, nil

	// if prefix == "" {
	//	return logger, nil
	//}
	//
	//prefLogger := zap.New(&prefixCore{Core: logger.Core(), prefix: strings.ToLower(prefix)})
	//
	//return prefLogger, nil
}

//type prefixCore struct {
//	zapcore.Core
//	prefix string
//}
//
//func (pc *prefixCore) With(fields []zapcore.Field) zapcore.Core {
//	for i := range fields {
//		fields[i].Key = pc.prefix + fields[i].Key
//	}
//
//	return &prefixCore{
//		Core:   pc.Core.With(fields),
//		prefix: pc.prefix,
//	}
//}
//
//func (pc *prefixCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
//	return pc.Core.Check(entry, checked)
//}
//
//func (pc *prefixCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
//	for i := range fields {
//		fields[i].Key = pc.prefix + fields[i].Key
//	}
//
//	return pc.Core.Write(entry, fields)
//}
