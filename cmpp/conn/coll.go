package conn

import (
	"sync"

	"github.com/a2dict/cmpp_conn/cmpp/protocol"
)

type OpHandler func(op protocol.Operation)

func NewOpHandlerStorage(size uint32) *OpHandlerStorage {
	return &OpHandlerStorage{
		size: size,
	}
}

type OpHandlerStorage struct {
	size uint32
	data sync.Map
}

func (s *OpHandlerStorage) Put(sequenceID uint32, handler OpHandler) {
	var idx uint32 = sequenceID % s.size
	s.data.Store(idx, handler)
}

func (s *OpHandlerStorage) Get(sequenceID uint32) OpHandler {
	if handler, ok := s.data.Load(sequenceID); ok {
		if hd, ok := handler.(OpHandler); ok {
			return hd
		}
	}
	return nil
}

func (s *OpHandlerStorage) Del(sequenceID uint32) {
	s.data.Delete(sequenceID)
}
