package pg2stats

import (
	"fmt"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"go.uber.org/zap"
)

type Metric interface {
	Name() string
	Value() float64
}

func NewGauge(name string, help string) *Gauge {
	return &Gauge{name: name, help: help, g: metrics.GetOrCreateGauge(string(name), nil)}
}

type Gauge struct {
	g    *metrics.Gauge
	name string
	help string
}

func (g *Gauge) Name() string {
	return g.name
}

func (g *Gauge) Value() float64 {
	return g.g.Get()
}

func (g *Gauge) Add(v float64) {
	g.g.Add(v)
}

func (g *Gauge) Set(v float64) {
	g.g.Set(v)
}

func NewCounter(name string, help string) *Counter {
	return &Counter{name: name, help: help, c: metrics.GetOrCreateFloatCounter(string(name))}
}

type Counter struct {
	c    *metrics.FloatCounter
	name string
	help string
}

func (c *Counter) Name() string {
	return c.name
}

func (c *Counter) Value() float64 {
	return c.c.Get()
}

func (c *Counter) Inc() {
	c.c.Add(1)
}

func (c *Counter) Add(v float64) {
	c.c.Add(v)
}

func NewCounterMap(nameBase, helpBase, labelName string, initLabels []string) *CounterMap {
	counters := make(map[string]*Counter)

	cm := &CounterMap{
		nameBase:  nameBase,
		labelName: labelName,
		helpBase:  helpBase,
		counters:  counters,
	}

	for _, labelValue := range initLabels {
		cm.Add(labelValue, 0)
	}

	return cm
}

type CounterMap struct {
	nameBase  string
	labelName string
	helpBase  string

	mu       sync.RWMutex
	counters map[string]*Counter
}

func (cm *CounterMap) Add(key string, v float64) {
	cm.mu.RLock()
	counter, ok := cm.counters[key]
	cm.mu.RUnlock()

	if !ok {
		cm.mu.Lock()
		counter, ok = cm.counters[key]
		if !ok {
			// Create new counter for dynamic labels
			label := fmt.Sprintf(`{%s="%s"}`, cm.labelName, key)
			help := fmt.Sprintf("%s (%s)", cm.helpBase, key)
			counter = NewCounter(cm.nameBase+label, help)
			cm.counters[key] = counter
		}
		cm.mu.Unlock()
	}

	counter.Add(v)
}

func (cm *CounterMap) GetStatsAsFields() []zap.Field {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var fields []zap.Field
	for _, counter := range cm.counters {
		fields = append(fields, zap.Float64(counter.name, counter.Value()))
	}

	return fields
}

type StatLogger interface {
	GetStatsAsFields() []zap.Field
}

type noLogger struct {
}

func (nl *noLogger) GetStatsAsFields() []zap.Field {
	return []zap.Field{}
}

func NewNop() StatLogger {
	return &noLogger{}
}

type Histogram struct {
	hist *metrics.PrometheusHistogram
}

func (h *Histogram) UpdateDuration(startTime time.Time) {
	h.hist.UpdateDuration(startTime)
}

func (h *Histogram) Update(v float64) {
	h.hist.Update(v)
}

func NewHistogram(name string) *Histogram {
	return &Histogram{hist: metrics.GetOrCreatePrometheusHistogram(name)}
}
