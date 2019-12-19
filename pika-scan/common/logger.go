package common

import (
	"os"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	MAX_SIZE         = 1800 // MB
	PATH_STRING_GLUE = "."
)

var (
	hostname = "127.0.0.1"
	logLevel = []zapcore.Level{
		zapcore.DebugLevel,
		zapcore.InfoLevel,
		zapcore.WarnLevel,
		zapcore.ErrorLevel,
		zapcore.FatalLevel,
	}
)

var LoggerWrapper *loggerWrapper = &loggerWrapper{}

type DefaultLoggerConfig struct {
	FilePath string
	FileName string
	Level    int
}

var DefaultLoggerConf *DefaultLoggerConfig = &DefaultLoggerConfig{
	FilePath: "./",
	FileName: "pikaScan-log",
	Level:    1,
}

func init() {
	h, err := os.Hostname()
	if err == nil {
		hostname = h
	}
}

type loggerWrapper struct {
	// loggerWrapper handle the function to create zap logger
}

func (l *loggerWrapper) createEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		NameKey:        "logger",
		TimeKey:        "timestamp",
		CallerKey:      "caller",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// CreateLevelEnablerFunc create level enabler function which
// indicate that logging the information only when the caller's
// level is higher than l
func (l *loggerWrapper) createLevelEnablerFunc(baseLevel zapcore.Level) zap.LevelEnablerFunc {
	return func(lvl zapcore.Level) bool {
		return lvl >= baseLevel
	}
}

func (l *loggerWrapper) createRotateWriteSyncer(fileName string) zapcore.WriteSyncer {
	return zapcore.AddSync(&lumberjack.Logger{
		Filename:  fileName,
		MaxSize:   MAX_SIZE,
		LocalTime: true,
		Compress:  true,
	})
}

func (l *loggerWrapper) createZapCore(fileName string, level zapcore.Level) zapcore.Core {
	return zapcore.NewCore(
		zapcore.NewJSONEncoder(l.createEncoderConfig()),
		l.createRotateWriteSyncer(fileName),
		l.createLevelEnablerFunc(level))
}

func (l *loggerWrapper) createLogger(core zapcore.Core) *zap.Logger {
	return zap.New(core, zap.AddCaller())
}

func (l *loggerWrapper) getShouldLoggedLevels(level int) []zapcore.Level {
	if level >= 0 && level <= 4 {
		return logLevel[level:]
	}
	return logLevel[1:]
}

func (l *loggerWrapper) createLoggerPath(baseLogPath, handlerName string) (string, error) {
	mask := syscall.Umask(0)
	defer syscall.Umask(mask)
	var logPath string
	if baseLogPath[len(baseLogPath)-1] == '/' {
		logPath = baseLogPath + handlerName + "/"
	} else {
		logPath = baseLogPath + "/" + handlerName + "/"
	}
	if err := os.MkdirAll(logPath, 0644); err != nil {
		return EMPTY_STRING, err
	}
	return logPath, nil
}

func (l *loggerWrapper) MustCreateLogger(baseLogPath, handlerName string, level int) *zap.Logger {
	logger, err := l.CreateLogger(baseLogPath, handlerName, level)
	if err != nil {
		panic("create Logger failed")
	}
	return logger
}

func (l *loggerWrapper) CreateLogger(baseLogPath, handlerName string, level int) (*zap.Logger, error) {
	var cores []zapcore.Core = make([]zapcore.Core, 0, len(logLevel))
	shouldLoggedLevels := l.getShouldLoggedLevels(level)
	for _, shouldLoggedLevel := range shouldLoggedLevels {
		logPath, err := l.createLoggerPath(baseLogPath, handlerName)
		if err != nil {
			return nil, err
		}
		cores = append(cores,
			l.createZapCore(logPath+handlerName+PATH_STRING_GLUE+hostname+PATH_STRING_GLUE+shouldLoggedLevel.String(),
				shouldLoggedLevel))
	}
	return l.createLogger(zapcore.NewTee(cores...)), nil
}
