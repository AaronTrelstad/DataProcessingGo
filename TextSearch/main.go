package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
)

type Trie struct {
	root *TrieNode
	mutex sync.Mutex
}

type TrieNode struct {
	children map[rune]*TrieNode
	docInfo map[int][]int
	isWord bool
}

type DocumentInfo struct {
	id int
	wordFreq map[string]int
	totalWords int
}

func BuildTree(files []string) *Trie {
	trie := &Trie{root: &TrieNode{children: make(map[rune]*TrieNode)}}

	var docs = make(map[int]*DocumentInfo)

	var waitGroup sync.WaitGroup
	var mutex sync.Mutex

	for docId, file := range files {
		waitGroup.Add(1)

		go func(docId int, file string) {
			defer waitGroup.Done()

			content := ReadFile(file)
			words := Tokenize(content)

			mutex.Lock()
			docs[docId] = &DocumentInfo{
				id: docId,
				wordFreq: make(map[string]int),
				totalWords: len(words),
			}
			mutex.Unlock()

			for pos, word := range words {
				trie.Insert(word, docId, pos)
				mutex.Lock()
				docs[docId].wordFreq[word]++
				mutex.Unlock()
			}
		}(docId, file)
	}

	waitGroup.Wait()

	return trie
}

func (t *Trie) Insert(word string, docId int, position int) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	node := t.root
	for _, char := range word {
		if node.children[char] == nil {
			node.children[char] = &TrieNode{children: make(map[rune]*TrieNode), docInfo: make(map[int][]int)}
		}
		node = node.children[char]
	}
	node.isWord = true
	node.docInfo[docId] = append(node.docInfo[docId], position)
}

func (t *Trie) Search(word string) map[int][]int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	node := t.root
	for _, char := range word {
		if node.children[char] == nil {
			return nil
		}
		node = node.children[char]
	}

	if node.isWord {
		return node.docInfo
	}

	return nil
}

func Tokenize(text string) []string {
	return strings.Fields(text)
}

func ReadFile(file string) string {
	content, err := os.ReadFile("texts/" + file)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return ""
	}
	return string(content) 
}

func TFIDF(word string, docId int, trie *Trie, docs map[int]*DocumentInfo) float64 {
	wordInfo := trie.Search(word)

	if wordInfo == nil {
		return 0.0
	}

	if freq, exists := wordInfo[docId]; exists {
		termFreq := float64(len(freq)) / float64(docs[docId].totalWords)
		inverseDocumentFrequency := math.Log(float64(len(docs)) / float64(1 + len(wordInfo)))

		return termFreq * inverseDocumentFrequency
	}

	return 0.0
}

func Search(query string, trie *Trie, docs map[int]*DocumentInfo) []int {
	queryWords := Tokenize(query)
	scores := make(map[int]float64)

	for _, word := range queryWords {
		wordInfo := trie.Search(word)

		if wordInfo == nil {
			continue
		}

		for docId := range wordInfo {
			scores[docId] += TFIDF(word, docId, trie, docs)
		}
	}

	results := make([]int, 0, len(scores))
	for docId := range scores {
		results = append(results, docId)
	}

	sort.Slice(results, func(i, j int) bool {
		return scores[results[i]] > scores[results[j]]
	})

	return results
}

func main() {
	files := []string{"1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt"}

	trie := BuildTree(files)

	docs := make(map[int]*DocumentInfo)
	for docId := range files {
		docs[docId] = &DocumentInfo{
			id: docId,
			wordFreq: make(map[string]int),
			totalWords: 0,
		}
	}

	query := "learning"
	results := Search(query, trie, docs)

	fmt.Println("Search Results for:", query)
	for _, docID := range results {
		fmt.Printf("Document ID: %d\n", docID)
	}
}
