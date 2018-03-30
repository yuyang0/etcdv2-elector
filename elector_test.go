package elector

import (
	"os"
	"testing"
)

var (
	e1, e2, e3 *Elector
	etcds      string
	err        error
)

const (
	TTL = 5
)

func init() {
	etcds = os.Getenv("ETCD_TEST")
	if etcds == "" {
		etcds = "http://127.0.0.1:2379"
	}
	e1, err = New(etcds, "/tmp/aaa/election", "127.0.0.1:1111", TTL)
	if err != nil {
		panic(err)
	}
	e2, err = New(etcds, "/tmp/aaa/election", "127.0.0.1:2222", TTL)
	if err != nil {
		panic(err)
	}
	e3, err = New(etcds, "/tmp/aaa/election", "127.0.0.1:3333", TTL)
	if err != nil {
		panic(err)
	}
}

func TestRunElection(t *testing.T) {
	ch1, err := e1.Run()
	if err != nil {
		panic(err)
	}
	ch2, err := e2.Run()
	if err != nil {
		panic(err)
	}
	ch3, err := e3.Run()
	if err != nil {
		panic(err)
	}
	t.Logf("e1: %s, e2: %s, e3: %s", e1.key, e2.key, e3.key)
	select {
	case <-ch1:
		t.Log("leader change to 1")
	case <-ch2:
		t.Error("expect 1 become leader, but got 2")
	case <-ch3:
		t.Error("expect 1 become leader, but got 3")
	}
	e1.Stop()
	select {
	case <-ch2:
		t.Log("leader change to 2")
	case <-ch3:
		t.Error("expect 2 become leader, but got 3")
	}
	e2.Stop()

	select {
	case <-ch3:
		t.Log("leader change to 3")
	}
}
