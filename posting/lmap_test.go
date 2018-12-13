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

package posting

import (
	"context"
	"math/rand"
	"testing"

	"github.com/golang/glog"

	"github.com/dgraph-io/dgraph/protos/pb"
	"github.com/dgraph-io/dgraph/x"
)

var dbInitialized bool
var predicateKeys [][]byte
var plistLen = 10000

func ensureDBInitialized(b *testing.B) {
	if !dbInitialized {
		glog.Infof("setting up the cluster")
		ctx := context.Background()
		startTs := uint64(1)

		txn := Oracle().RegisterStartTs(startTs)
		// put data into the pstore with 1M different posting lists
		numPlists := 100
		attr := "name"

		for i := 0; i < numPlists; i++ {
			randomKey := x.DataKey(attr, uint64(i))
			// cache the plist associated with key so that it can be committed later
			l, err := Get(randomKey)
			if err != nil {
				b.Error(err)
			}

			for j := 0; j < plistLen; j++ {
				edge := &pb.DirectedEdge{
					ValueId: uint64(j),
					Label:   "testing",
					Op:      pb.DirectedEdge_SET,
				}

				if err = l.AddMutation(ctx, txn, edge); err != nil {
					b.Error(err)
				}
			}

			predicateKeys = append(predicateKeys, randomKey)
		}

		writer := x.NewTxnWriter(pstore)
		txn.CommitToDisk(writer, 1)
		writer.Flush()
		b.Logf("done committing to disk")

		dbInitialized = true
	}
}

func BenchmarkGet(b *testing.B) {
	ensureDBInitialized(b)
	// run go routines in parallel trying to get keys from the LRU
	x.LcacheHit.Set(0)
	x.LcacheMiss.Set(0)
	x.LcacheRace.Set(0)
	lcache.Reset()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			randomKeyIndex := rand.Intn(len(predicateKeys))
			randomKey := predicateKeys[randomKeyIndex]
			plist, err := Get(randomKey)
			x.Check(err)
			actualPLen := plist.Length(2, 0)
			if actualPLen != plistLen {
				b.Fatalf("the plist should have a length of %d, got %d instead", plistLen, actualPLen)
			}
		}
	})
	glog.Infof("cache hit:%d (%f), cache miss:%d, cache race:%d", x.LcacheHit.Value(),
		(float64(x.LcacheHit.Value()) / float64(b.N)),
		x.LcacheMiss.Value(), x.LcacheRace.Value())
	glog.Flush()
}
