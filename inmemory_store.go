package raft_kvdb

import (
	"errors"
	"github.com/hashicorp/raft"
	"sync"
)

var (
	noError error = nil
)

type InmemStore struct {
	mutex sync.RWMutex

	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]*raft.Log
	kv        map[string][]byte
	kvInt     map[string]uint64
}

func NewInmemStore() *InmemStore {
	return &InmemStore{
		logs:  make(map[uint64]*raft.Log),
		kv:    make(map[string][]byte),
		kvInt: make(map[string]uint64),
	}
}

func (i *InmemStore) Set(key []byte, val []byte) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.kv[string(key)] = val
	return noError
}

func (i *InmemStore) Get(key []byte) ([]byte, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	value, ok := i.kv[string(key)]
	if !ok {
		return nil, errors.New("Not found")
	}
	return value, noError
}

func (i *InmemStore) SetUint64(key []byte, val uint64) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.kvInt[string(key)] = val

	return noError
}

func (i *InmemStore) GetUint64(key []byte) (uint64, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	return i.kvInt[string(key)], nil
}

func (i *InmemStore) FirstIndex() (uint64, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	return i.lowIndex, noError
}

func (i *InmemStore) LastIndex() (uint64, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	return i.highIndex, noError
}

func (i *InmemStore) GetLog(index uint64, log *raft.Log) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	key, ok := i.logs[index]
	if !ok {
		return raft.ErrLogNotFound
	}
	*key = *log
	return noError
}

func (i *InmemStore) StoreLog(log *raft.Log) error {
	return i.StoreLogs([]*raft.Log{log})
}

func (i *InmemStore) StoreLogs(logs []*raft.Log) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for _, l := range logs {
		nextLog := logs[l.Index]
		if i.lowIndex == 0 {
			i.lowIndex = nextLog.Index
		}

		if i.highIndex < nextLog.Index {
			i.highIndex = nextLog.Index
		}

	}
	return noError
}

func (i *InmemStore) DeleteRange(min, max uint64) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for j := min; j <= max; j++ {
		delete(i.logs, j)
	}

	if min <= i.lowIndex {
		i.lowIndex = max + 1
	}
	if max >= i.highIndex {
		i.highIndex = min - 1
	}
	if i.lowIndex > i.highIndex {
		i.lowIndex = 0
		i.highIndex = 0
	}

	return noError
}
