// Copyright (c) 2019, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"

	"github.com/onsi/gomega"

	"github.com/3JoB/goleveldb/storage"
	"github.com/3JoB/goleveldb/testutil"
)

func TestGetOverlaps(t *testing.T) {
	gomega.RegisterTestingT(t)
	stor := testutil.NewStorage()
	defer stor.Close()
	s, err := newSession(stor, nil)
	if err != nil {
		t.Fatal(err)
	}

	v := newVersion(s)
	v.newStaging()

	tmp := make([]byte, 4)
	mik := func(i uint64, typ keyType, ukey bool) []byte {
		if i == 0 {
			return nil
		}
		binary.BigEndian.PutUint32(tmp, uint32(i))
		if ukey {
			key := make([]byte, 4)
			copy(key, tmp)
			return key
		}
		return []byte(makeInternalKey(nil, tmp, 0, typ))
	}

	rec := &sessionRecord{}
	for i, f := range []struct {
		min   uint64
		max   uint64
		level int
	}{
		// Overlapped level 0 files
		{min: 1, max: 8, level: 0},
		{min: 4, max: 5, level: 0},
		{min: 6, max: 10, level: 0},
		// Non-overlapped level 1 files
		{min: 2, max: 3, level: 1},
		{min: 8, max: 10, level: 1},
		{min: 13, max: 13, level: 1},
		{min: 20, max: 100, level: 1},
	} {
		rec.addTable(f.level, int64(i), 1, mik(f.min, keyTypeVal, false), mik(f.max, keyTypeVal, false))
	}
	vs := v.newStaging()
	vs.commit(rec)
	v = vs.finish(false)

	for i, x := range []struct {
		min      uint64
		max      uint64
		level    int
		expected []int64
	}{
		// Level0 cases
		{min: 0, max: 0, level: 0, expected: []int64{2, 1, 0}},
		{min: 1, max: 0, level: 0, expected: []int64{2, 1, 0}},
		{min: 0, max: 10, level: 0, expected: []int64{2, 1, 0}},
		{min: 2, max: 7, level: 0, expected: []int64{2, 1, 0}},

		// Level1 cases
		{min: 1, max: 1, level: 1, expected: nil},
		{min: 0, max: 100, level: 1, expected: []int64{3, 4, 5, 6}},
		{min: 5, max: 0, level: 1, expected: []int64{4, 5, 6}},
		{min: 5, max: 4, level: 1, expected: nil}, // invalid search space
		{min: 1, max: 13, level: 1, expected: []int64{3, 4, 5}},
		{min: 2, max: 13, level: 1, expected: []int64{3, 4, 5}},
		{min: 3, max: 13, level: 1, expected: []int64{3, 4, 5}},
		{min: 4, max: 13, level: 1, expected: []int64{4, 5}},
		{min: 4, max: 19, level: 1, expected: []int64{4, 5}},
		{min: 4, max: 20, level: 1, expected: []int64{4, 5, 6}},
		{min: 4, max: 100, level: 1, expected: []int64{4, 5, 6}},
		{min: 4, max: 105, level: 1, expected: []int64{4, 5, 6}},
	} {
		tf := v.levels[x.level]
		res := tf.getOverlaps(nil, s.icmp, mik(x.min, keyTypeSeek, true), mik(x.max, keyTypeSeek, true), x.level == 0)

		var fnums []int64
		for _, f := range res {
			fnums = append(fnums, f.fd.Num)
		}
		if !reflect.DeepEqual(x.expected, fnums) {
			t.Errorf("case %d failed, expected %v, got %v", i, x.expected, fnums)
		}
	}
}

func BenchmarkGetOverlapLevel0(b *testing.B) {
	benchmarkGetOverlap(b, 0, 500000)
}

func BenchmarkGetOverlapNonLevel0(b *testing.B) {
	benchmarkGetOverlap(b, 1, 500000)
}

func benchmarkGetOverlap(b *testing.B, level int, size int) {
	stor := storage.NewMemStorage()
	defer stor.Close()
	s, err := newSession(stor, nil)
	if err != nil {
		b.Fatal(err)
	}

	v := newVersion(s)
	v.newStaging()

	tmp := make([]byte, 4)
	mik := func(i uint64, typ keyType, ukey bool) []byte {
		if i == 0 {
			return nil
		}
		binary.BigEndian.PutUint32(tmp, uint32(i))
		if ukey {
			key := make([]byte, 4)
			copy(key, tmp)
			return key
		}
		return []byte(makeInternalKey(nil, tmp, 0, typ))
	}

	rec := &sessionRecord{}
	for i := 1; i <= size; i++ {
		min := mik(uint64(2*i), keyTypeVal, false)
		max := mik(uint64(2*i+1), keyTypeVal, false)
		rec.addTable(level, int64(i), 1, min, max)
	}
	vs := v.newStaging()
	vs.commit(rec)
	v = vs.finish(false)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		files := v.levels[level]
		start := rand.Intn(size)
		end := rand.Intn(size-start) + start
		files.getOverlaps(nil, s.icmp, mik(uint64(2*start), keyTypeVal, true), mik(uint64(2*end), keyTypeVal, true), level == 0)
	}
}
