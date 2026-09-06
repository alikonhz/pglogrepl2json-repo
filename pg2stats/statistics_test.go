package pg2stats

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMultiGauge(t *testing.T) {
	var wg sync.WaitGroup

	const (
		num     = 10_000
		workers = 1_000
	)

	wg.Add(workers)

	gauge := NewGauge("test_gauge", "test")

	for g := 0; g < workers; g++ {
		go func() {
			for i := 0; i < num; i++ {
				gauge.Add(float64(1))
			}

			defer wg.Done()
		}()
	}

	wg.Wait()

	actual := gauge.Value()
	expected := float64(num * workers)

	assert.Equal(t, expected, actual)
}

func TestMultiCounter(t *testing.T) {
	var wg sync.WaitGroup

	const (
		num     = 10_000
		workers = 1_000
	)

	wg.Add(workers)

	counter := NewCounter("test_counter", "test")

	for g := 0; g < workers; g++ {
		go func() {
			for i := 0; i < num; i++ {
				counter.Add(float64(1))
			}

			defer wg.Done()
		}()
	}

	wg.Wait()

	actual := counter.Value()
	expected := float64(num * workers)

	assert.Equal(t, expected, actual)
}

func TestMultiCounterMap(t *testing.T) {
	var wg sync.WaitGroup

	const (
		num     = 1_000
		workers = 64
	)

	counterMap := NewCounterMap(
		fmt.Sprintf("test_counter_map_%d", time.Now().UnixNano()),
		"test",
		"label",
		nil,
	)
	labels := []string{
		"label_0",
		"label_1",
		"label_2",
		"label_3",
		"label_4",
		"label_5",
		"label_6",
		"label_7",
	}

	done := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = counterMap.GetStatsAsFields()
			}
		}
	}()

	wg.Add(workers)
	for g := 0; g < workers; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				counterMap.Add(labels[(g+i)%len(labels)], 1)
			}
		}()
	}

	wg.Wait()
	close(done)
	readerWG.Wait()

	fields := counterMap.GetStatsAsFields()
	assert.Len(t, fields, len(labels))

	var actual float64
	for _, field := range fields {
		actual += math.Float64frombits(uint64(field.Integer))
	}

	expected := float64(num * workers)
	assert.Equal(t, expected, actual)
}
