// logger.go
package logger

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type Level int32

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

type Manager struct {
	level atomic.Int32
	debug bool
}

func NewManager(debug bool) *Manager {
	m := &Manager{}
	m.level.Store(int32(INFO))
	if debug {
		m.debug = debug
		m.level.Store(int32(DEBUG))
	}

	return m
}

func (m *Manager) SetLevel(level Level) *Manager {
	m.level.Store(int32(level))

	return m
}

// formatArgs formats key-value pairs in a more readable way
func formatArgs(args ...interface{}) string {
	if len(args) == 0 {
		return ""
	}

	var pairs []string
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			pairs = append(pairs, fmt.Sprintf("%v=%v", args[i], args[i+1]))
		}
	}

	return " " + strings.Join(pairs, " ")
}

func (m *Manager) Debug(msg string, args ...interface{}) {
	if m.debug && m.level.Load() <= int32(DEBUG) {
		fmt.Printf("DEBUG: %s%s\n", msg, formatArgs(args...))
	}
}

func (m *Manager) Info(msg string, args ...interface{}) {
	if m.level.Load() <= int32(INFO) {
		fmt.Printf("INFO: %s%s\n", msg, formatArgs(args...))
	}
}

func (m *Manager) Warn(msg string, args ...interface{}) {
	if m.level.Load() <= int32(WARN) {
		fmt.Printf("WARN: %s%s\n", msg, formatArgs(args...))
	}
}

func (m *Manager) Error(msg string, args ...interface{}) {
	if m.level.Load() <= int32(ERROR) {
		fmt.Printf("ERROR: %s%s\n", msg, formatArgs(args...))
	}
}

// structured logging
type Fields map[string]interface{}

func (m *Manager) WithFields(fields Fields) *Event {
	return &Event{
		manager: m,
		fields:  fields,
	}
}

type Event struct {
	manager *Manager
	fields  Fields
}

func (e *Event) formatFields() string {
	var pairs []string
	for k, v := range e.fields {
		pairs = append(pairs, fmt.Sprintf("%v=%v", k, v))
	}
	return " " + strings.Join(pairs, " ")
}

func (e *Event) Debug(msg string) {
	if e.manager.debug && e.manager.level.Load() <= int32(DEBUG) {
		fmt.Printf("DEBUG: %s%s\n", msg, e.formatFields())
	}
}

func (e *Event) Info(msg string) {
	if e.manager.level.Load() <= int32(INFO) {
		fmt.Printf("INFO: %s%s\n", msg, e.formatFields())
	}
}

func (e *Event) Warn(msg string) {
	if e.manager.level.Load() <= int32(WARN) {
		fmt.Printf("WARN: %s%s\n", msg, e.formatFields())
	}
}

func (e *Event) Error(msg string) {
	if e.manager.level.Load() <= int32(ERROR) {
		fmt.Printf("ERROR: %s%s\n", msg, e.formatFields())
	}
}
