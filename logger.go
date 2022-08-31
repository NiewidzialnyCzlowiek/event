package event

import "go.uber.org/zap"

type LoggerImpl = *zap.SugaredLogger

type LoggerFactory interface {
	NewLogger() LoggerImpl
}

type DefaultLoggerFactory struct {
}

func NewDefaultLoggerFactory() *DefaultLoggerFactory {
	return &DefaultLoggerFactory{}
}

func (lf *DefaultLoggerFactory) NewLogger() LoggerImpl {
	log, _ := zap.NewProduction()
	return log.Sugar()
}
