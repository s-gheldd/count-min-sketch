package hash

import (
	"math/rand"
	"unsafe"
)

type Function func(key uint32) uint32

type Provider []Function

const (
	m1 = 0xcc9e2d51
	m2 = 0x1b873593
	m3 = 0x85ebca6b
	m4 = 0xc2b2ae35
)

func Murmur(size int) Provider {
	rand.Seed(1)
	funcs := make([]Function, 0, size)
	for index := 0; index < size; index++ {
		//seed
		seed := rand.Uint32()
		funcs = append(funcs, mur(seed))
	}
	return funcs
}

func mur(seed uint32) func(key uint32) uint32 {
	return func(key uint32) uint32 {
		data := (*[4]byte)(unsafe.Pointer(&key))
		h1 := seed
		k1 := uint32(data[0]) | (uint32(data[1]) << 8) | (uint32(data[2]) << 16) | (uint32(data[3]) << 24)
		k1 *= m1
		k1 = (k1 << 15) | (k1 >> 17) // ROTL32(k1,15);
		k1 *= m2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // ROTL32(h1,13);
		h1 = h1*5 + 0xe6546b64

		// finalization
		h1 ^= 4

		// fmix(h1);
		h1 ^= h1 >> 16
		h1 *= m3
		h1 ^= h1 >> 13
		h1 *= m4
		h1 ^= h1 >> 16
		return h1
	}
}
