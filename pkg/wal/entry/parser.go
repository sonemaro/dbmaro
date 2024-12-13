package entry

import (
	"fmt"
	"sync"
)

var entryPool = sync.Pool{
	New: func() interface{} {
		return &Entry{}
	},
}

type Parser struct {
	headerPool sync.Pool
	bufferPool sync.Pool
}

func NewParser() *Parser {
	return &Parser{
		headerPool: sync.Pool{
			New: func() interface{} {
				return &Header{}
			},
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 4096)
			},
		},
	}
}

func (p *Parser) Parse(data []byte) (*Entry, int, error) {
	if len(data) < HeaderSize {
		return nil, 0, fmt.Errorf("insufficient data for header")
	}

	header := p.headerPool.Get().(*Header)
	defer p.headerPool.Put(header)

	if err := header.Unmarshal(data[:HeaderSize]); err != nil {
		return nil, 0, fmt.Errorf("header unmarshal: %w", err)
	}

	totalSize := HeaderSize + int(header.DataSize)
	if len(data) < totalSize {
		return nil, 0, fmt.Errorf("insufficient data for entry")
	}

	entry := entryPool.Get().(*Entry)
	if err := entry.Unmarshal(data[HeaderSize:totalSize]); err != nil {
		entryPool.Put(entry)
		return nil, 0, fmt.Errorf("entry unmarshal: %w", err)
	}

	return entry, totalSize, nil
}
