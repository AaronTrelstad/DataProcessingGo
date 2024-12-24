package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

type ItemData struct {
	unitsSold  int
	totalProfit float64
}

type Items struct {
	itemMutex sync.Mutex
	data      map[string]*ItemData
}

var currentLine = 0
var lineMutex sync.Mutex

func readCSV(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	if _, err := reader.Read(); err != nil {
		return nil, fmt.Errorf("error reading CSV header: %v", err)
	}

	lines, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV lines: %v", err)
	}

	return lines, nil
}

func (results *Items) addData(itemType string, unitsSold int, totalProfit float64) {
	results.itemMutex.Lock()
	defer results.itemMutex.Unlock()

	if _, exists := results.data[itemType]; !exists {
		results.data[itemType] = &ItemData{}
	}

	results.data[itemType].unitsSold += unitsSold
	results.data[itemType].totalProfit += totalProfit
}

func (results *Items) processChunk(start int, end int, lines [][]string) {
	for i := start; i < end; i++ {
		line := lines[i]

		itemType := line[2]
		unitsSold, err := strconv.Atoi(line[8])
		if err != nil {
			fmt.Printf("Error parsing units sold for line %d: %v\n", i, err)
			continue
		}

		totalProfit, err := strconv.ParseFloat(line[12], 64)
		if err != nil {
			fmt.Printf("Error parsing total profit for line %d: %v\n", i, err)
			continue
		}

		results.addData(itemType, unitsSold, totalProfit)
	}
}

func worker(id int, chunkSize int, lines [][]string, results *Items, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for {
		lineMutex.Lock()
		start := currentLine
		currentLine += chunkSize
		lineMutex.Unlock()

		if start >= len(lines) {
			break
		}

		end := start + chunkSize
		if end > len(lines) {
			end = len(lines)
		}

		results.processChunk(start, end, lines)
	}
}

func main() {
	start := time.Now()

	results := &Items{
		data: make(map[string]*ItemData),
	}

	const filename = "SalesData.csv"
	lines, err := readCSV(filename)
	if err != nil {
		fmt.Println(err)
		return
	}

	const threads = 20
	const chunkSize = 100000

	var waitGroup sync.WaitGroup

	for i := 0; i < threads; i++ {
		waitGroup.Add(1)
		go worker(i, chunkSize, lines, results, &waitGroup)
	}

	waitGroup.Wait()

	elapsed := time.Since(start)

	fmt.Println("Elapsed time:", elapsed)

	fmt.Println("Item Type  |  Units Sold  |  Total Profit")
	for itemType, data := range results.data {
		fmt.Printf("%s | %d | $%.2f\n", itemType, data.unitsSold, data.totalProfit)
	}
}
