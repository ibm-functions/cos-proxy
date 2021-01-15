package logger

import (
	"fmt"
	"runtime"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.Logger
	ZapLogLevel zapcore.Level
}

var logger *Logger // Singleton instance

func GetLogger() *Logger {
	var err error

	if logger == nil {
		logger, err = NewLogger(GetConfig().Level)
		if err != nil {
			panic(err)
		}
	}

	return logger
}

func (l *Logger) Print(v ...interface{}) {
	str := fmt.Sprintf("[sarama] %s", fmt.Sprint(v...))
	l.forceLog(zapcore.DebugLevel, str)
}

func (l *Logger) Printf(format string, v ...interface{}) {
	str := fmt.Sprintf("[sarama] %s", fmt.Sprintf(format, v...))
	l.forceLog(zapcore.DebugLevel, str)
}

func (l *Logger) Println(v ...interface{}) {
	str := fmt.Sprintf("[sarama] %s", fmt.Sprint(v...))
	l.forceLog(zapcore.DebugLevel, str)
}

func (l *Logger) forceLog(level zapcore.Level, msg string, logArgs ...zap.Field) {
	// We need a "checked entry" do do the Write(), so checking that the logger is
	// set to a level (ErrorLevel) that supports the least amount of logging will
	// guarantee a CheckedEntry is returned.  Override fields in the returned
	// CheckedEntry to cause it to produce the correct log level output
	if ce := l.Logger.Check(zapcore.ErrorLevel, msg); ce != nil {
		// Since the zap logger is wrappered, bump the call stack setting to
		// log the correct caller's file and line number
		ce.Caller = zapcore.NewEntryCaller(runtime.Caller(2))
		ce.Level = level
		ce.Write(logArgs...)
	}
}

func NewLogger(logLevel string) (*Logger, error) {
	atomicLevel, err := getLogLevel(logLevel)
	if err != nil {
		return nil, err
	}

	zapLogLevel, err := MakeZapLogLevel(logLevel)
	if err != nil {
		return nil, err
	}

	config := zap.NewProductionConfig()
	config.Level = atomicLevel
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.MessageKey = "message" // Elasticsearch message field name, zap default is "msg"
	config.DisableStacktrace = true

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return &Logger{logger, zapLogLevel}, nil
}

func getLogLevel(logLevel string) (zap.AtomicLevel, error) {
	switch strings.ToLower(logLevel) {
	case "", "debug":
		return zap.NewAtomicLevelAt(zap.DebugLevel), nil
	case "info":
		return zap.NewAtomicLevelAt(zap.InfoLevel), nil
	case "warn":
		return zap.NewAtomicLevelAt(zap.WarnLevel), nil
	case "error":
		return zap.NewAtomicLevelAt(zap.ErrorLevel), nil
	case "fatal":
		return zap.NewAtomicLevelAt(zap.FatalLevel), nil
	default:
		return zap.NewAtomicLevelAt(zap.DebugLevel), invalidLogLevel(logLevel)
	}
}

func invalidLogLevel(logLevel string) error {
	return fmt.Errorf("Invalid log level %q", logLevel)
}

func MakeZapLogLevel(logLevel string) (zapcore.Level, error) {
	var l zapcore.Level
	err := l.UnmarshalText([]byte(logLevel))
	return l, err
}
