package main

import (
	"errors"
	"fmt"
)

type DAG struct {
	adjList map[string][]string;
	inDegree map[string]int;
}

func NewDAG() *DAG {
	return &DAG{
		adjList: make(map[string][]string),
		inDegree: make(map[string]int),
	}
}

func (dag *DAG) AddTask(task string, dependencies []string) {
	if _, exists := dag.adjList[task]; !exists {
		dag.adjList[task] = []string{};
	}

	for _, dependency := range dependencies {
		dag.adjList[dependency] = append(dag.adjList[dependency], task);
		dag.inDegree[task]++;
	}

	if _, exists := dag.inDegree[task]; !exists {
		dag.inDegree[task] = 0;
	}
}

func (dag *DAG) TopologicalSort() ([]string, error) {
	var order []string;
	queue := []string{};

	for task, degree := range dag.inDegree {
		if degree == 0 {
			queue = append(queue, task);
		}
	}

	for len(queue) > 0 {
		task := queue[0];
		queue = queue[1:];

		order = append(order, task);

		for _, neighbor := range dag.adjList[task] {
			dag.inDegree[neighbor]--;
			if dag.inDegree[neighbor] == 0 {
				queue = append(queue, neighbor);
			}
		}
	}

	if len(order) != len(dag.adjList) {
		return nil, errors.New("graph contains a cycle");
	}

	return order, nil;
}

func ExecuteTasks(order []string) {
	fmt.Println("Executing Order: ", order);
}

func main() {
	dag := NewDAG();

	// Models ETL (Extract, Transform, Load)

	// Extract
	dag.AddTask("download_logs", []string{})
	dag.AddTask("download_metadata", []string{})

	// Transform
	dag.AddTask("parse_logs", []string{"download_logs"})
	dag.AddTask("filter_logs", []string{"parse_logs", "download_metadata"})
	dag.AddTask("aggregate_logs", []string{"filter_logs"})

	// Loads
	dag.AddTask("upload_aggregates", []string{"aggregate_logs"})
	dag.AddTask("update_dashboard", []string{"upload_aggregates"})

	order, err := dag.TopologicalSort();
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	ExecuteTasks(order);
}
