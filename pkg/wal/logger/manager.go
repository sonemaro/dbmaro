package logger

import (
	"fmt"
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

func (m *Manager) Debug(msg string, args ...interface{}) {
	if m.debug && m.level.Load() <= int32(DEBUG) {
		fmt.Printf("DEBUG: "+msg+"\n", args...)
	}
}

func (m *Manager) Info(msg string, args ...interface{}) {
	if m.level.Load() <= int32(INFO) {
		fmt.Printf("INFO: "+msg+"\n", args...)
	}
}

func (m *Manager) Warn(msg string, args ...interface{}) {
	if m.level.Load() <= int32(WARN) {
		fmt.Printf("WARN: "+msg+"\n", args...)
	}
}

func (m *Manager) Error(msg string, args ...interface{}) {
	if m.level.Load() <= int32(ERROR) {
		fmt.Printf("ERROR: "+msg+"\n", args...)
	}
}

// Structured logging
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

func (e *Event) Debug(msg string) {
	if e.manager.debug && e.manager.level.Load() <= int32(DEBUG) {
		fmt.Printf("DEBUG: %s %v\n", msg, e.fields)
	}
}

func (e *Event) Info(msg string) {
	if e.manager.level.Load() <= int32(INFO) {
		fmt.Printf("INFO: %s %v\n", msg, e.fields)
	}
}

func (e *Event) Warn(msg string) {
	if e.manager.level.Load() <= int32(WARN) {
		fmt.Printf("WARN: %s %v\n", msg, e.fields)
	}
}

func (e *Event) Error(msg string) {
	if e.manager.level.Load() <= int32(ERROR) {
		fmt.Printf("ERROR: %s %v\n", msg, e.fields)
	}
}
