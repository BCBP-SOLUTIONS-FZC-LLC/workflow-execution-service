package main

import (
	"fmt"
	"sync"
	"testing"

	"go.temporal.io/sdk/worker"
)

type wkbootIdentifiedWorker struct {
	worker.Worker
	id int
}

func TestWorkerRegistry_ConcurrentAddHasAndAllLoseNoWorkerAndHandOutSnapshots(t *testing.T) {
	const goroutines = 32
	registry := newWorkerRegistry()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("queue-%d", i)
			registry.add(name, &wkbootIdentifiedWorker{id: i})
			if !registry.has(name) {
				t.Errorf("has(%q) = false right after add", name)
			}
			_ = registry.has("never-added")
			_ = registry.all()
		}(i)
	}
	close(start)
	wg.Wait()

	all := registry.all()
	if len(all) != goroutines {
		t.Fatalf("all() holds %d workers, want %d", len(all), goroutines)
	}
	seen := map[int]bool{}
	for _, w := range all {
		seen[w.(*wkbootIdentifiedWorker).id] = true
	}
	for i := 0; i < goroutines; i++ {
		if !seen[i] {
			t.Errorf("worker %d is missing from all()", i)
		}
		if !registry.has(fmt.Sprintf("queue-%d", i)) {
			t.Errorf("has(queue-%d) = false after every add finished", i)
		}
	}
	if registry.has("never-added") {
		t.Error("has(never-added) = true")
	}

	registry.add("late-queue", &wkbootIdentifiedWorker{id: goroutines})
	for _, w := range all {
		if w.(*wkbootIdentifiedWorker).id == goroutines {
			t.Error("the slice returned earlier by all() contains a worker added afterwards: it is not a snapshot")
		}
	}
	if got := len(registry.all()); got != goroutines+1 {
		t.Errorf("all() after the late add = %d workers, want %d", got, goroutines+1)
	}
}
