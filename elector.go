package elector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
)

var (
	KMissingError    = errors.New("No such key")
	KNilNodeError    = errors.New("Etcd Store returns a nil node")
	KDirNodeError    = errors.New("Etcd Store returns this is a directory node")
	KNonDirNodeError = errors.New("Etcd Store returns this is a non-directory node")
)

type Elector struct {
	etcdAddr string
	dir      string
	val      string
	ttl      int
	isLeader bool
	keysAPI  client.KeysAPI
	leaderCh chan bool
	watchCh  chan bool
	stopCh   chan bool
	key      string
	ctx      context.Context
	sync.RWMutex
}

func New(addr, dir, val string, ttl int) (*Elector, error) {
	ctx := context.Background()
	c, err := client.New(client.Config{
		Endpoints: strings.Split(addr, ","),
	})
	if err != nil {
		return nil, err
	}
	s := &Elector{
		dir:      dir,
		val:      val,
		ttl:      ttl,
		keysAPI:  client.NewKeysAPI(c),
		ctx:      ctx,
		leaderCh: make(chan bool),
		watchCh:  make(chan bool),
		stopCh:   make(chan bool),
		isLeader: false,
	}
	return s, nil
}

// Stop the elector
func (elt *Elector) Stop() {
	close(elt.stopCh)
	close(elt.leaderCh)
}

func (elt *Elector) Leader() (string, error) {
	keys, err := elt.keysInDir(elt.dir)
	if err != nil {
		return "", err
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return "", fmt.Errorf("there are no members")
	}
	resp, err := elt.keysAPI.Get(elt.ctx, keys[0], nil)
	if err != nil {
		return "", err
	}
	return resp.Node.Value, nil
}

// Run the elector
func (elt *Elector) Run() (chan bool, error) {
	if _, err := elt.createOrderKey(); err != nil {
		return nil, err
	}
	go elt.refreshTTL()
	go elt.watch()
	return elt.leaderCh, nil
}

func (elt *Elector) refreshTTL() {
	for {
		elt.RLock()
		key := elt.key
		elt.RUnlock()
		opts := &client.SetOptions{
			PrevExist: client.PrevExist,
			TTL:       time.Duration(elt.ttl) * time.Second,
			Refresh:   true,
		}
		ticker := time.NewTicker(time.Duration(elt.ttl-1) * time.Second)
	L:
		for {
			select {
			case <-ticker.C:
				_, err := elt.keysAPI.Set(elt.ctx, key, "", opts)
				// key expire, try to recreate the key
				if client.IsKeyNotFound(err) {
					if _, err := elt.createOrderKey(); err != nil {
						return
					}
					// notify watcher to reload
					elt.stopCh <- true
				} else if err != nil {
					fmt.Printf("refresh error %s, %v\n", key, err)
					break L
				}
			case <-elt.stopCh:
				return
			}
		}
	}
}

func (elt *Elector) watch() {
	for {
		elt.RLock()
		key := elt.key
		elt.RUnlock()

		ctx := context.Background()
		var cancel context.CancelFunc

		keys, err := elt.keysInDir(elt.dir)
		if err != nil {
			return
		}
		sort.Strings(keys)
		idx := sort.SearchStrings(keys, key)

		if idx > 0 {
			idx--
		}
		prevKey := keys[idx]
		if prevKey == key {
			elt.leaderCh <- true
		} else {
			ctx, cancel = context.WithCancel(ctx)
			go func(ctx context.Context) {
				wch := elt.keysAPI.Watcher(prevKey, nil)
				for {
					resp, err := wch.Next(ctx)
					if err != nil {
						fmt.Printf("watch error %s, %v\n", key, err)
						// notify to reload watcher
						elt.watchCh <- true
						return
					}
					if resp.Action == "expire" {
						// notify to reload watcher
						elt.watchCh <- true
						break
					}
				}
			}(ctx)
		}
		select {
		case <-elt.stopCh:
			if cancel != nil {
				cancel()
			}
			return
		case <-elt.watchCh:
			// reload watcher
			if cancel != nil {
				cancel()
			}
		}
	}
}

func (elt *Elector) keysInDir(dir string) ([]string, error) {
	keys := make([]string, 0)
	if resp, err := elt.keysAPI.Get(elt.ctx, dir, &client.GetOptions{Quorum: true}); err != nil {
		if cerr, ok := err.(client.Error); ok && cerr.Code == client.ErrorCodeKeyNotFound {
			return keys, KMissingError
		}
		return keys, err
	} else {
		if resp.Node == nil {
			return keys, KNilNodeError
		}
		if !resp.Node.Dir {
			return keys, KNonDirNodeError
		}
		for _, node := range resp.Node.Nodes {
			if node != nil {
				keys = append(keys, node.Key)
			}
		}
	}
	return keys, nil
}

func (elt *Elector) createOrderKey() (string, error) {
	elt.Lock()
	defer elt.Unlock()

	opts := &client.CreateInOrderOptions{
		TTL: time.Duration(elt.ttl) * time.Second,
	}
	resp, err := elt.keysAPI.CreateInOrder(elt.ctx, elt.dir, elt.val, opts)
	if err != nil {
		return "", err
	}
	elt.key = resp.Node.Key
	return resp.Node.Key, nil
}
