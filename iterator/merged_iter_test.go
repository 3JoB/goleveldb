// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/3JoB/goleveldb/comparer"
	. "github.com/3JoB/goleveldb/iterator"
	"github.com/3JoB/goleveldb/testutil"
)

var _ = testutil.Defer(func() {
	Describe("Merged iterator", func() {
		Test := func(filled int, empty int) func() {
			return func() {
				It("Should iterates and seeks correctly", func(done Done) {
					rnd := testutil.NewRand()

					// Build key/value.
					filledKV := make([]testutil.KeyValue, filled)
					kv := testutil.KeyValue_Generate(nil, 100, 1, 1, 10, 4, 4)
					kv.Iterate(func(i int, key, value []byte) {
						filledKV[rnd.Intn(filled)].Put(key, value)
					})

					// Create itearators.
					iters := make([]Iterator, filled+empty)
					for i := range iters {
						if empty == 0 || (rnd.Int()%2 == 0 && filled > 0) {
							filled--
							Expect(filledKV[filled].Len()).ShouldNot(BeZero())
							iters[i] = NewArrayIterator(filledKV[filled])
						} else {
							empty--
							iters[i] = NewEmptyIterator(nil)
						}
					}

					// Test the iterator.
					t := testutil.IteratorTesting{
						KeyValue: kv.Clone(),
						Iter:     NewMergedIterator(iters, comparer.DefaultComparer, true),
					}
					testutil.DoIteratorTesting(&t)
					done <- true
				}, 15.0)
			}
		}

		Describe("with three, all filled iterators", Test(3, 0))
		Describe("with one filled, one empty iterators", Test(1, 1))
		Describe("with one filled, two empty iterators", Test(1, 2))
	})
})

func benchmarkMergedIteratorN(b *testing.B, n int) {
	iters := make([]Iterator, n)
	for i := range iters {
		kv := testutil.KeyValue_Generate(nil, 100, 1, 1, 10, 4, 4)
		iters[i] = NewArrayIterator(kv)
	}

	mi := NewMergedIterator(iters, comparer.DefaultComparer, true)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mi.First()
		for mi.Next() {
			mi.Key()
		}
	}
}

func BenchmarkMergedIterator(b *testing.B) {
	b.Run("2 iters", func(b *testing.B) {
		benchmarkMergedIteratorN(b, 2)
	})

	b.Run("50 iters", func(b *testing.B) {
		benchmarkMergedIteratorN(b, 50)
	})
}