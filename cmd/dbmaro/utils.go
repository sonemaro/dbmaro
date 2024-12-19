// cmd/dbmaro/utils.go
package main

import (
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
)

func parseLogLevel(level string) logger.Level {
	switch level {
	case "debug":
		return logger.DEBUG
	case "info":
		return logger.INFO
	case "warn":
		return logger.WARN
	case "error":
		return logger.ERROR
	default:
		return logger.INFO
	}
}
