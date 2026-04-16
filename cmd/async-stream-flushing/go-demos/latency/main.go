package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const msgCount = 10_000

func measureLatencies(ctx context.Context, js jetstream.JetStream, subject string, payload []byte) []time.Duration {
	latencies := make([]time.Duration, 0, msgCount)
	for i := range msgCount {
		start := time.Now()
		if _, err := js.Publish(ctx, subject, payload); err != nil {
			log.Fatalf("Publish to %s failed at msg %d: %v", subject, i, err)
		}
		latencies = append(latencies, time.Since(start))
	}
	return latencies
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printHistogram(label string, latencies []time.Duration) {
	slices.Sort(latencies)

	p50 := percentile(latencies, 0.50)
	p90 := percentile(latencies, 0.90)
	p99 := percentile(latencies, 0.99)
	max := latencies[len(latencies)-1]

	fmt.Printf("  %s\n", label)
	fmt.Printf("    p50:  %10s\n", p50.Round(time.Microsecond))
	fmt.Printf("    p90:  %10s\n", p90.Round(time.Microsecond))
	fmt.Printf("    p99:  %10s\n", p99.Round(time.Microsecond))
	fmt.Printf("    max:  %10s\n", max.Round(time.Microsecond))

	// Visual histogram — bucket latencies into ranges
	buckets := []struct {
		label string
		max   time.Duration
	}{
		{"< 50µs ", 50 * time.Microsecond},
		{"< 100µs", 100 * time.Microsecond},
		{"< 250µs", 250 * time.Microsecond},
		{"< 500µs", 500 * time.Microsecond},
		{"<   1ms", 1 * time.Millisecond},
		{"<   5ms", 5 * time.Millisecond},
		{"<  10ms", 10 * time.Millisecond},
		{">= 10ms", time.Duration(1<<63 - 1)},
	}

	counts := make([]int, len(buckets))
	for _, l := range latencies {
		for b, bucket := range buckets {
			if l < bucket.max || b == len(buckets)-1 {
				counts[b]++
				break
			}
		}
	}

	fmt.Println()
	maxCount := 0
	for _, c := range counts {
		if c > maxCount {
			maxCount = c
		}
	}

	barWidth := 40
	for i, bucket := range buckets {
		if counts[i] == 0 {
			continue
		}
		bar := int(float64(counts[i]) / float64(maxCount) * float64(barWidth))
		if bar == 0 && counts[i] > 0 {
			bar = 1
		}
		pct := float64(counts[i]) / float64(len(latencies)) * 100
		fmt.Printf("    %s │%s %4.1f%% (%d)\n",
			bucket.label,
			strings.Repeat("█", bar),
			pct, counts[i])
	}
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create streams
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "SYNC_DEMO",
		Subjects: []string{"sync.>"},
		Storage:  jetstream.FileStorage,
		Replicas: 1,
	})
	if err != nil {
		log.Fatalf("Failed to create sync stream: %v", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        "ASYNC_DEMO",
		Subjects:    []string{"async.>"},
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		PersistMode: jetstream.AsyncPersistMode,
	})
	if err != nil {
		log.Fatalf("Failed to create async stream: %v", err)
	}

	payload := []byte(strings.Repeat("X", 1024))

	fmt.Printf("Measuring per-message publish latency (%d messages each)...\n\n", msgCount)

	fmt.Println("  Publishing to sync stream...")
	syncLatencies := measureLatencies(ctx, js, "sync.bench", payload)

	fmt.Println("  Publishing to async stream...")
	asyncLatencies := measureLatencies(ctx, js, "async.bench", payload)

	fmt.Println()
	fmt.Println(strings.Repeat("─", 60))
	printHistogram("Sync (fsync per message)", syncLatencies)
	fmt.Println()
	printHistogram("Async (deferred fsync)", asyncLatencies)
	fmt.Println(strings.Repeat("─", 60))

	// Summary
	slices.Sort(syncLatencies)
	slices.Sort(asyncLatencies)
	sp50 := percentile(syncLatencies, 0.50)
	ap50 := percentile(asyncLatencies, 0.50)

	fmt.Println()
	fmt.Println("  What this shows:")
	fmt.Println("  With default (sync) persist, every publish blocks on fsync —")
	fmt.Println("  the server waits for the disk to confirm the write before")
	fmt.Println("  sending your ack. With async persist, the ack comes back")
	fmt.Println("  before fsync, so publish latency drops.")
	fmt.Println()
	if ap50 < sp50 {
		reduction := float64(sp50-ap50) / float64(sp50) * 100
		fmt.Printf("  On this machine, async cuts median latency by ~%.0f%%\n", reduction)
		fmt.Printf("  (%s → %s per message).\n", sp50.Round(time.Microsecond), ap50.Round(time.Microsecond))
	} else {
		fmt.Printf("  On this machine (fast SSD), both modes show similar latency\n")
		fmt.Printf("  (%s sync vs %s async).\n", sp50.Round(time.Microsecond), ap50.Round(time.Microsecond))
	}
	fmt.Println("  The gap widens on slower storage (cloud VMs, spinning disks).")

	// Cleanup
	_ = js.DeleteStream(ctx, "SYNC_DEMO")
	_ = js.DeleteStream(ctx, "ASYNC_DEMO")
	fmt.Println("\n  Cleaned up: deleted both streams")
}
