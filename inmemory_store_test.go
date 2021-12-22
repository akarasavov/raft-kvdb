package raft_kvdb

import (
	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/raft"
	"testing"
)

func TestShouldAbleToGetStoredLogs(t *testing.T) {

	i := NewInmemStore()

	storedLog := raft.Log{0, 2, raft.LogCommand, make([]byte, 0)}
	err := i.StoreLog(&storedLog)
	if err != noError {
		t.Fatalf("Wasn't able to store log")
	}

	var log raft.Log
	i.GetLog(storedLog.Index, &log)

	if cmp.Equal(storedLog, log) {
		t.Fatalf("Read log is not equal to the stored one")
	}

	if i.lowIndex != 0 {
		t.Fatalf("Wrong low index")
	}

	if i.highIndex != 0 {
		t.Fatalf("Wrong high index")
	}

}