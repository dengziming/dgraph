/*
 * Copyright 2015-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package benchmark

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgraph/posting"

	"github.com/golang/glog"

	"github.com/dgraph-io/dgraph/protos/pb"
	"github.com/dgraph-io/dgraph/x"
)

var predicateKeys [][]byte

func TestMain(m *testing.M) {
	flag.Parse()
	dir := posting.SetupStore()
	initDB()
	r := m.Run()
	os.RemoveAll(dir)
	os.Exit(r)
}

var plistLen = 100000

const MB = 1024 * 1024

func initDB() {
	//if !dbInitialized {
	predicateKeys = [][]byte{}
	fmt.Println("logging")
	glog.Infof("-----------------------\nsetting up the cluster")
	ctx := context.Background()
	startTs := uint64(1)

	txn := posting.Oracle().RegisterStartTs(startTs)
	// put data into the pstore with 1M different posting lists
	numUidsPerList := 100
	attr := "name"

	for i := 0; i < plistLen; i++ {
		randomKey := x.DataKey(attr, uint64(i))
		// cache the plist associated with key so that it can be committed later
		l, err := posting.Get(randomKey)
		x.Check(err)

		for j := 0; j < numUidsPerList; j++ {
			edge := &pb.DirectedEdge{
				ValueId: uint64(j),
				Label:   "testing",
				Op:      pb.DirectedEdge_SET,
			}

			if err = l.AddMutation(ctx, txn, edge); err != nil {
				x.Check(err)
			}
		}

		if i%10000 == 0 {
			writer := x.NewTxnWriter(posting.Pstore)
			x.Check(txn.CommitToDisk(writer, 1))
			writer.Flush()

			x.Check(txn.CommitToMemory(1))
			glog.Infof("committed to disk i:%d", i)
		}

		predicateKeys = append(predicateKeys, randomKey)
	}

	glog.Infof("done committing to disk with %d keys, cache size %d", len(predicateKeys),
		atomic.LoadUint64(&posting.Lcache.CurSize))
}

func BenchmarkGet(b *testing.B) {
	b.SetParallelism(100)
	// clear cache to avoid influence from previous runs of this function
	posting.Lcache.Reset()

	x.LcacheHit.Set(0)
	x.LcacheMiss.Set(0)
	x.LcacheRace.Set(0)

	//oldCacheSize := atomic.LoadUint64(&posting.Lcache.CurSize)
	// limit the cache size to 1/10 of required size to enable evictions
	posting.Lcache.UpdateMaxSize(MB)
	// ensure cache is empty before reading data
	posting.Lcache.Reset()

	//b.ResetTimer()
	numGetsPerGoRoutine := 100
	// run go routines in parallel trying to get keys from the LRU
	glog.Infof("benchmark with N:%d", b.N)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numGetsPerGoRoutine; i++ {
				randomKeyIndex := rand.Intn(len(predicateKeys))
				randomKey := predicateKeys[randomKeyIndex]
				_, err := posting.Get(randomKey)
				x.Check(err)
			}
		}
	})
	glog.Infof("cache hit:%d (%f), cache miss:%d, cache race:%d, evicts: %d",
		x.LcacheHit.Value(),
		(float64(x.LcacheHit.Value()) / float64(b.N)),
		x.LcacheMiss.Value(), x.LcacheRace.Value(), posting.Lcache.Evicts)
	glog.Flush()
}
