package hash

import "unsafe"

const (
	MURMURHASH2_DIGEST_LENGTH = 4
)

/*
 * -----------------------------------------------------------------------------
 * MurmurHash2, by Austin Appleby
 * Note - This code makes a few assumptions about how your machine behaves -
 * 1. We can read a 4-byte value from any address without crashing
 * 2. sizeof(int) == 4
 * And it has a few limitations -
 * 1. It will not work incrementally.
 * 2. It will not produce the same results on little-endian and big-endian
 *    machines.
 */

func MurmurHash2(key []byte, seed uint32) uint32 {
	/*
	   	 * 'm' and 'r' are mixing constants generated offline.
	    * They're not really 'magic', they just happen to work well.
	*/

	m := uint32(0x5bd1e995)
	r := uint32(24)
	dataLen := uint32(len(key))

	/* Initialize the hash to a 'random' value */

	h := uint32(seed ^ dataLen)

	/* Mix 4 bytes at a time into the hash */

	data := key

	i := 0
	for {
		if dataLen < 4 {
			break
		}
		k := *(*uint32)(unsafe.Pointer(&data[i*4]))

		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k

		//		data += 4;
		i += 1
		dataLen -= 4
	}

	/* Handle the last few bytes of the input array */

	switch dataLen {
	case 3:
		h ^= uint32(data[2]) << 16
		h ^= uint32(data[1]) << 8
		h ^= uint32(data[0])
		h *= m

	case 2:
		h ^= uint32(data[1]) << 8
		h ^= uint32(data[0])
		h *= m
	case 1:
		h ^= uint32(data[0])
		h *= m
	default:
	}

	/* Do a few final mixes of the hash to ensure the last few
	 * bytes are well-incorporated. */

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}