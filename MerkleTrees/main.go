package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type FileRecord struct {
	Filename     string `json:"filename"`
	MerkleRoot   string `json:"merkle_root"`
	LastModified string `json:"last_modified"`
}

const storageFile = "file_hashes.json";

func ComputeMerkleRoot(filename string, blockCount int) (string, error) {
	fileData, err := os.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %v", filename, err)
	}

	blockSize := (len(fileData) + blockCount - 1) / blockCount 
	var blocks [][]byte
	for i := 0; i < len(fileData); i += blockSize {
		end := i + blockSize
		if end > len(fileData) {
			end = len(fileData)
		}
		blocks = append(blocks, fileData[i:end])
	}

	var hashes [][]byte
	for _, block := range blocks {
		hash := sha256.Sum256(block)
		hashes = append(hashes, hash[:])
	}

	for len(hashes) > 1 {
		var newHashes [][]byte
		for i := 0; i < len(hashes); i += 2 {
			if i+1 < len(hashes) {
				combined := append(hashes[i], hashes[i+1]...)
				newHash := sha256.Sum256(combined)
				newHashes = append(newHashes, newHash[:])
			} else {
				newHashes = append(newHashes, hashes[i])
			}
		}
		hashes = newHashes
	}

	return hex.EncodeToString(hashes[0]), nil
}
func SaveRecord(record FileRecord) error {
	var records []FileRecord;

	if _, err := os.Stat(storageFile); err == nil {
		data, err := os.ReadFile(storageFile);

		if err != nil {
			return fmt.Errorf("failed to read storage file")
		}
		if err := json.Unmarshal(data, &records); err != nil {
			return fmt.Errorf("failed to parse storage file")
		}
	}

	for i, rec := range records {
		if rec.Filename == record.Filename {
			records[i] = record
			data, err := json.MarshalIndent(records, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to encode updated records")
			}
			return os.WriteFile(storageFile, data, 0644)
		}
	}

	records = append(records, record)
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode records")
	}

	return os.WriteFile(storageFile, data, 0644)
}

func CheckFileIntegrity(filename string, blocks int) error {
	var records []FileRecord;

	data, err := os.ReadFile(storageFile);
	if err != nil {
		return fmt.Errorf("failed to read storage file");
	}
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("failed to parse storage file");
	}

	currentRoot, err := ComputeMerkleRoot(filename, blocks);
	if err != nil {
		return err;
	}

	for _, rec := range records {
		if rec.Filename == filename {
			if rec.MerkleRoot == currentRoot {
				fmt.Println("File is not modified");
			} else {
				fmt.Println("File is modified");
			}
			return nil;
		}
	}

	fmt.Println("File not found in records.");
	return nil;
}

func main() {
	filename := "example.txt"
	const blocks = 7;

	root, err := ComputeMerkleRoot(filename, blocks)
	if err != nil {
		fmt.Printf("error computing merkle root")
		return
	}

	record := FileRecord{
		Filename:     filename,
		MerkleRoot:   root,
		LastModified: time.Now().Format(time.RFC3339),
	}

	err = CheckFileIntegrity(filename, blocks)
	if err != nil {
		fmt.Printf("error checking file integrity")
	}

	err = SaveRecord(record)
	if err != nil {
		fmt.Printf("error saving record")
		return
	}
}


