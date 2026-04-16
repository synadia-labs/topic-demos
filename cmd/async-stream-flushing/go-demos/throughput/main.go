package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const msgCount = 50_000

func publishWithProgress(ctx context.Context, js jetstream.JetStream, label, subject string, payload []byte) time.Duration {
	var count atomic.Int64

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	done := make(chan struct{})
	start := time.Now()

	go func() {
		for {
			select {
			case <-ticker.C:
				c := count.Load()
				elapsed := time.Since(start).Seconds()
				rate := float64(0)
				if elapsed > 0 {
					rate = float64(c) / elapsed
				}
				fmt.Printf("\r  %s: %6d / %d  (%6.0f msg/s)", label, c, msgCount, rate)
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < msgCount; i++ {
		if _, err := js.Publish(ctx, subject, payload); err != nil {
			log.Fatalf("Publish to %s failed at msg %d: %v", subject, i, err)
		}
		count.Add(1)
	}
	elapsed := time.Since(start)
	close(done)

	rate := float64(msgCount) / elapsed.Seconds()
	fmt.Printf("\r  %s: %6d / %d  (%6.0f msg/s)  [%s]\n", label, msgCount, msgCount, rate, elapsed.Round(time.Millisecond))
	return elapsed
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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Create sync stream (default persist mode — fsync on every write)
	syncStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "SYNC_DEMO",
		Subjects: []string{"sync.>"},
		Storage:  jetstream.FileStorage,
		Replicas: 1,
	})
	if err != nil {
		log.Fatalf("Failed to create sync stream: %v", err)
	}
	fmt.Printf("Created stream %q (persist_mode: default)\n", syncStream.CachedInfo().Config.Name)

	// Create async stream (ack before fsync)
	asyncStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        "ASYNC_DEMO",
		Subjects:    []string{"async.>"},
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		PersistMode: jetstream.AsyncPersistMode,
	})
	if err != nil {
		log.Fatalf("Failed to create async stream: %v", err)
	}
	fmt.Printf("Created stream %q (persist_mode: async)\n\n", asyncStream.CachedInfo().Config.Name)

	// 1KB payload — larger writes make fsync cost more visible
	payload := []byte(strings.Repeat("X", 1024))

	// Synchronous publish, one at a time, wait for ack.
	// This is the pattern where fsync cost directly impacts throughput:
	//   sync:  publish → write → fsync → ack → next
	//   async: publish → write → ack → next  (fsync in background)
	fmt.Println("Publishing to sync stream (fsync per message)...")
	syncDuration := publishWithProgress(ctx, js, "Sync ", "sync.bench", payload)

	fmt.Println("\nPublishing to async stream (deferred fsync)...")
	asyncDuration := publishWithProgress(ctx, js, "Async", "async.bench", payload)

	// Results
	syncRate := float64(msgCount) / syncDuration.Seconds()
	asyncRate := float64(msgCount) / asyncDuration.Seconds()

	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────┐")
	fmt.Printf("  │  Sync  (default)  %8.0f msg/s            │\n", syncRate)
	fmt.Printf("  │  Async            %8.0f msg/s            │\n", asyncRate)
	fmt.Println("  └─────────────────────────────────────────────┘")
	if syncDuration > 0 {
		speedup := float64(syncDuration) / float64(asyncDuration)
		fmt.Printf("\n  → Async is %.1fx faster (no per-message fsync)\n", speedup)
	}

	// Verify
	syncInfo, _ := syncStream.Info(ctx)
	asyncInfo, _ := asyncStream.Info(ctx)
	fmt.Printf("\n  SYNC_DEMO:  %d messages\n", syncInfo.State.Msgs)
	fmt.Printf("  ASYNC_DEMO: %d messages\n", asyncInfo.State.Msgs)

	// Cleanup
	_ = js.DeleteStream(ctx, "SYNC_DEMO")
	_ = js.DeleteStream(ctx, "ASYNC_DEMO")
	fmt.Println("\n  Cleaned up: deleted both streams")

	fmt.Println("\n  Note: speedup varies by disk type. Fast SSDs have very cheap")
	fmt.Println("  fsync, so the gap may be modest. On spinning disks or network-")
	fmt.Println("  attached storage (common in cloud VMs), fsync is much slower")
	fmt.Println("  and async mode can be 3-5x faster.")
	fmt.Println()
	fmt.Println("  For a per-message latency breakdown (p50/p90/p99 + histogram),")
	fmt.Println("  run: go run ../latency/")
}
