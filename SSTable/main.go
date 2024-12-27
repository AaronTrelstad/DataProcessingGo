package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

type SSTable struct {
	file       *os.File
	filename   string
	index      map[string]int64
	bloomFilter *BloomFilter
}

type BloomFilter struct {
	size         int
	bitArray     []bool
	hashFunctions []func(string) uint32
}

func NewBloomFilter(size int, numHashes int) *BloomFilter {
	bf := &BloomFilter{
		bitArray: make([]bool, size),
		size: size,
	}

	for i := 0; i < numHashes; i++ {
		bf.hashFunctions = append(bf.hashFunctions, generateHashFunction(i))
	}

	return bf
}

func generateHashFunction(index int) func(string) uint32 {
	return func(s string) uint32 {
		hash := fnv.New32a()
		hash.Write([]byte(fmt.Sprintf("%d-%s", index, s)))
        return hash.Sum32()
	}
}

func (b *BloomFilter) Add(value string) {
	for _, hashFunction := range b.hashFunctions {
		hash := hashFunction(value)
		index := int(hash % uint32(b.size))
		b.bitArray[index] = true
	}
}

func (b *BloomFilter) Check(value string) bool {
	for _, hashFunction := range b.hashFunctions {
		hash := hashFunction(value)
		index := int(hash % uint32(b.size))
		if !b.bitArray[index] {
			return false
		}
	}
	return true
}

func NewSSTable(filename string, bloomFilterSize int) (*SSTable, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	bloomFilter := NewBloomFilter(bloomFilterSize, 4)

	return &SSTable{
		filename:   filename,
		file:       file,
		index:      make(map[string]int64),
		bloomFilter: bloomFilter,
	}, nil
}

func (s *SSTable) Set(key, value string) error {
	keyBytes := []byte(key)
	valueBytes := []byte(value)

	keySize := int32(len(keyBytes))
	valueSize := int32(len(valueBytes))

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, keySize)
	buf.Write(keyBytes)
	binary.Write(buf, binary.LittleEndian, valueSize)
	buf.Write(valueBytes)

	_, err = s.file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	s.index[key] = offset

	s.bloomFilter.Add(key)
	return nil
}

func (s *SSTable) Get(key string) (string, error) {
	if !s.bloomFilter.Check(key) {
		return "", fmt.Errorf("Key not found in Bloom filter")
	}

	offset, exists := s.index[key]
	if !exists {
		return "", fmt.Errorf("Key not found in index")
	}

	_, err := s.file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	var keySize, valueSize int32
	err = binary.Read(s.file, binary.LittleEndian, &keySize)
	if err != nil {
		return "", err
	}

	keyBytes := make([]byte, keySize)
	_, err = s.file.Read(keyBytes)
	if err != nil {
		return "", err
	}

	err = binary.Read(s.file, binary.LittleEndian, &valueSize)
	if err != nil {
		return "", err
	}

	valueBytes := make([]byte, valueSize)
	_, err = s.file.Read(valueBytes)
	if err != nil {
		return "", err
	}

	return string(valueBytes), nil
}

func main() {
	store, err := NewSSTable("test.db", 1000)
	if err != nil {
		fmt.Println("Error creating SSTable:", err)
		return
	}
	defer store.file.Close()

	err = store.Set("Test1", "1")
	if err != nil {
		fmt.Println("Error writing:", err)
		return
	}

	err = store.Set("Test2", "2")
	if err != nil {
		fmt.Println("Error writing:", err)
		return
	}

	value, err := store.Get("Test2")
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	fmt.Printf("Value: %s\n", value)
}
