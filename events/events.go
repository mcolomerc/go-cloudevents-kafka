package event // package event

import (
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

// Create structs with random injected data
type Event struct {
	Hostname    string    `json:"hostname"`
	AllocMemory string    `json:"allocmemory"`
	TotalAlloc  string    `json:"totalalloc"`
	System      string    `json:"system"`
	NumGC       string    `json:"numgc"`
	MemUsage    string    `json:"memusage"`
	CPU         string    `json:"cpu"`
	Created     time.Time `json:"created"`
}

func New() *Event {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	hostname, _ := os.Hostname()

	return &Event{
		Hostname:    hostname,
		AllocMemory: fmt.Sprint(m.Alloc),
		TotalAlloc:  fmt.Sprint(bToMb(m.TotalAlloc)),
		System:      fmt.Sprint(m.Sys),
		NumGC:       fmt.Sprint(m.NumGC),
		MemUsage:    fmt.Sprint(getMemoryUsage()),
		CPU:         fmt.Sprint(getCpuUsage()),
		Created:     time.Now(),
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func getCpuUsage() int {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		log.Fatal(err)
	}
	return int(math.Ceil(percent[0]))
}

func getMemoryUsage() int {
	memory, err := mem.VirtualMemory()
	if err != nil {
		log.Fatal(err)
	}
	return int(math.Ceil(memory.UsedPercent))
}
