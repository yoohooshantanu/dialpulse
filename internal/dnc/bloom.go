package dnc

import (
	"hash/fnv"
	"math"
	"strings"
	"sync"
)

// BloomFilter is a thread-safe, high-performance bitset Bloom filter for DNC phone compliance
type BloomFilter struct {
	mu        sync.RWMutex
	bits      []uint64
	numBits   uint64
	numHashes uint32
	count     uint64
}

// NewBloomFilter creates a Bloom filter optimized for target capacity and false-positive rate
func NewBloomFilter(expectedItems uint64, falsePositiveRate float64) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1000000
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1.0 {
		falsePositiveRate = 0.001 // 0.1% false positive rate
	}

	// m = - (n * ln(p)) / (ln(2)^2)
	m := -float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)
	numBits := uint64(math.Ceil(m))
	// Align to 64-bit boundaries
	words := (numBits + 63) / 64
	numBits = words * 64

	// k = (m / n) * ln(2)
	k := (float64(numBits) / float64(expectedItems)) * math.Ln2
	numHashes := uint32(math.Ceil(k))
	if numHashes == 0 {
		numHashes = 1
	}

	return &BloomFilter{
		bits:      make([]uint64, words),
		numBits:   numBits,
		numHashes: numHashes,
	}
}

// NormalizeNumber strips formatting to E.164 digits format (e.g. "+1 (555) 123-4567" -> "15551234567")
func NormalizeNumber(phone string) string {
	var b strings.Builder
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	res := b.String()
	// Strip leading US country code if 11 digits starting with 1 to normalize to 10 digits
	if len(res) == 11 && res[0] == '1' {
		res = res[1:]
	}
	return res
}

func hashPair(data string) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(data))
	hash1 := h1.Sum64()

	// Secondary hash using variant seed
	h2 := fnv.New64()
	_, _ = h2.Write([]byte(data + "_dnc_salt"))
	hash2 := h2.Sum64()

	return hash1, hash2
}

// Add inserts a phone number into the DNC filter
func (bf *BloomFilter) Add(phone string) {
	norm := NormalizeNumber(phone)
	if len(norm) == 0 {
		return
	}

	h1, h2 := hashPair(norm)

	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := uint32(0); i < bf.numHashes; i++ {
		// Kirsch-Mitzenmacher double-hashing technique
		combined := (h1 + uint64(i)*h2) % bf.numBits
		word := combined / 64
		bit := combined % 64
		bf.bits[word] |= (1 << bit)
	}
	bf.count++
}

// Contains checks if a phone number exists in the DNC filter
func (bf *BloomFilter) Contains(phone string) bool {
	norm := NormalizeNumber(phone)
	if len(norm) == 0 {
		return false
	}

	h1, h2 := hashPair(norm)

	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i := uint32(0); i < bf.numHashes; i++ {
		combined := (h1 + uint64(i)*h2) % bf.numBits
		word := combined / 64
		bit := combined % 64
		if (bf.bits[word] & (1 << bit)) == 0 {
			return false
		}
	}
	return true
}

// Count returns number of items inserted
func (bf *BloomFilter) Count() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.count
}
