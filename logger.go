package dvara

import (
	"go.uber.org/zap"
	"fmt"
	"go.uber.org/zap/zapcore"
	"time"
	"os"
)

// Logger allows for simple text logging.
type Logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Warning(args ...interface{})
	Warningf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

type AppLogger struct {
	logger *zap.Logger
}

func (l *AppLogger) Error(args ...interface{}) {
	l.logger.Error(fmt.Sprintf("%s", args[0]))
}
func (l *AppLogger) Errorf(format string, args ...interface{}) {
	l.logger.Error(fmt.Sprintf(format, args...))
}
func (l *AppLogger) Warning(args ...interface{}) {
	l.logger.Warn(fmt.Sprintf("%s", args[0]))
}
func (l *AppLogger) Warningf(format string, args ...interface{}) {
	l.logger.Warn(fmt.Sprintf(format, args...))
}
func (l *AppLogger) Info(args ...interface{}) {
	l.logger.Info(fmt.Sprintf("%s", args[0]))
}
func (l *AppLogger) Infof(format string, args ...interface{}) {
	l.logger.Info(fmt.Sprintf(format, args...))
}
func (l *AppLogger) Debug(args ...interface{}) {
	l.logger.Debug(fmt.Sprintf("%s", args[0]))
}
func (l *AppLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debug(fmt.Sprintf(format, args...))
}

var logger *zap.Logger

func InitLogger(config *Conf) (*AppLogger) {

	cfg := config.LogConfig

	cfg.DisableCaller = true
	cfg.EncoderConfig = zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     func (t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(time.Now().Format(time.RFC3339Nano))
		},
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	hostname := ""
	name, err := os.Hostname(); if err == nil {
		hostname = name
	}
	cfg.InitialFields = map[string]interface{}{"app": config.App, "type": "dvara", "deployment": config.Deployment, "hostname": hostname}

	logger, _ = cfg.Build()

	return &AppLogger{logger: logger}
}