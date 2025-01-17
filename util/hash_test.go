// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package util

import (
	"testing"
)

var hashTests = []struct {
	data []byte
	seed uint32
	hash uint32
}{
	{data: nil, seed: 0xbc9f1d34, hash: 0xbc9f1d34},
	{data: []byte{0x62}, seed: 0xbc9f1d34, hash: 0xef1345c4},
	{data: []byte{0xc3, 0x97}, seed: 0xbc9f1d34, hash: 0x5b663814},
	{data: []byte{0xe2, 0x99, 0xa5}, seed: 0xbc9f1d34, hash: 0x323c078f},
	{data: []byte{0xe1, 0x80, 0xb9, 0x32}, seed: 0xbc9f1d34, hash: 0xed21633a},
	{data: []byte{
		0x01, 0xc0, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x14, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x04, 0x00,
		0x00, 0x00, 0x00, 0x14,
		0x00, 0x00, 0x00, 0x18,
		0x28, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}, seed: 0x12345678, hash: 0xf333dabb},
}

func TestHash(t *testing.T) {
	for i, x := range hashTests {
		h := Hash(x.data, x.seed)
		if h != x.hash {
			t.Fatalf("test-%d: invalid hash, %#x vs %#x", i, h, x.hash)
		}
	}
}
