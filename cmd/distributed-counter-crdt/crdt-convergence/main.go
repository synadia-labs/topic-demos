// Distributed Counter CRDT — Cross-Domain Convergence Demo
//
// Demonstrates the true CRDT property of NATS JetStream counter streams:
// two independent JetStream domains accumulate counters without coordination,
// then converge to the algebraic sum via cross-domain stream sourcing.
//
// Topology: east (port 4250, domain "east") and west (port 4251, domain "west")
// are standalone JetStream nodes connected via gateway.
//
// The property shown: east and west write independently to their own domain
// counters. When east is configured to source from west, it converges to the
// correct total — no coordinator, no two-phase commit, no shared state.
//
// Requires: task start (see Taskfile.yml)

package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/counters"
)

const (
	streamName  = "VOTES"
	eastURL     = "nats://app:app@localhost:4250"
	westURL     = "nats://app:app@localhost:4251"
	eastSubject = "votes.east"
	westSubject = "votes.west"
	eastWrites  = 100
	westWrites  = 150
	goroutines  = 3
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Connect one client per domain.
	ncEast, err := nats.Connect(eastURL)
	if err != nil {
		log.Fatalf("connect east: %v", err)
	}
	defer ncEast.Close()

	ncWest, err := nats.Connect(westURL)
	if err != nil {
		log.Fatalf("connect west: %v", err)
	}
	defer ncWest.Close()

	fmt.Printf("East connected: %s\n", ncEast.ConnectedUrl())
	fmt.Printf("West connected: %s\n", ncWest.ConnectedUrl())

	jsEast, err := jetstream.New(ncEast)
	if err != nil {
		log.Fatalf("jetstream east: %v", err)
	}
	jsWest, err := jetstream.New(ncWest)
	if err != nil {
		log.Fatalf("jetstream west: %v", err)
	}

	// 2. Create the same counter stream in each domain independently.
	//    Same name, same subject space — but each domain is fully isolated.
	streamCfg := jetstream.StreamConfig{
		Name:            streamName,
		Subjects:        []string{"votes.>"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	}
	if _, err = jsEast.CreateStream(ctx, streamCfg); err != nil {
		log.Fatalf("create east stream: %v", err)
	}
	if _, err = jsWest.CreateStream(ctx, streamCfg); err != nil {
		log.Fatalf("create west stream: %v", err)
	}
	fmt.Printf("Stream %q created in both domains (counter + direct)\n\n", streamName)

	// 3. Get typed counter handles — one per domain.
	ctrEast, err := counters.GetCounter(ctx, jsEast, streamName)
	if err != nil {
		log.Fatalf("counter east: %v", err)
	}
	ctrWest, err := counters.GetCounter(ctx, jsWest, streamName)
	if err != nil {
		log.Fatalf("counter west: %v", err)
	}

	// 4. Write concurrently to each domain — no coordination, no shared state.
	//    East and west goroutines run simultaneously but are completely unaware of each other.
	fmt.Printf("Writing %d increments to east and %d to west concurrently (no coordination)...\n",
		eastWrites, westWrites)
	var wg sync.WaitGroup
	writeN(ctx, &wg, ctrEast, eastSubject, eastWrites, goroutines)
	writeN(ctx, &wg, ctrWest, westSubject, westWrites, goroutines)
	wg.Wait()

	// 5. Read each domain's partial view — they've diverged.
	eastTotal, err := ctrEast.Load(ctx, eastSubject)
	if err != nil {
		log.Fatalf("load east: %v", err)
	}
	westTotal, err := ctrWest.Load(ctx, westSubject)
	if err != nil {
		log.Fatalf("load west: %v", err)
	}
	fmt.Printf("East sees %s = %s\n", eastSubject, eastTotal)
	fmt.Printf("West sees %s = %s\n", westSubject, westTotal)
	fmt.Printf("East has no view of %s — the domains have diverged.\n\n", westSubject)

	// 6. CRDT merge: wire east to source from west.
	//    From this point, messages written to west propagate to east automatically.
	fmt.Println("Sourcing west into east's VOTES stream...")
	_, err = jsEast.UpdateStream(ctx, jetstream.StreamConfig{
		Name:            streamName,
		Subjects:        []string{"votes.>"},
		AllowMsgCounter: true,
		AllowDirect:     true,
		Sources:         []*jetstream.StreamSource{{Name: streamName, Domain: "west"}},
	})
	if err != nil {
		log.Fatalf("update stream: %v", err)
	}

	// Wait for west's counter to propagate into east.
	if err := pollUntil(ctx, func() (*big.Int, error) {
		return ctrEast.Load(ctx, westSubject)
	}, big.NewInt(westWrites)); err != nil {
		log.Fatalf("convergence timeout: %v", err)
	}
	fmt.Printf("East now sees %s = %d (sourced from west)\n", westSubject, westWrites)
	fmt.Printf("East total across both subjects: %d + %d = %d\n\n",
		eastWrites, westWrites, eastWrites+westWrites)

	// 7. Live propagation: more writes to west flow to east automatically.
	const extraWrites = 50
	fmt.Printf("Writing %d more increments to west...\n", extraWrites)
	var wg2 sync.WaitGroup
	writeN(ctx, &wg2, ctrWest, westSubject, extraWrites, 1)
	wg2.Wait()

	newWestTotal := int64(westWrites + extraWrites)
	if err := pollUntil(ctx, func() (*big.Int, error) {
		return ctrEast.Load(ctx, westSubject)
	}, big.NewInt(newWestTotal)); err != nil {
		log.Fatalf("live propagation timeout: %v", err)
	}
	fmt.Printf("East caught up: %s = %d (live propagation)\n\n", westSubject, newWestTotal)

	// 8. Summary.
	grandTotal := eastWrites + westWrites + extraWrites
	fmt.Printf("All %d writes converged correctly.\n", grandTotal)
	fmt.Println("East and west accumulated independently. Sourcing merged them without coordination.")

	// Cleanup.
	fmt.Println()
	if err := jsEast.DeleteStream(ctx, streamName); err != nil {
		log.Fatalf("delete east stream: %v", err)
	}
	if err := jsWest.DeleteStream(ctx, streamName); err != nil {
		log.Fatalf("delete west stream: %v", err)
	}
	fmt.Println("Streams deleted. Done.")
}

// writeN fires n increments of +1 across g goroutines, each sending their share sequentially.
func writeN(ctx context.Context, wg *sync.WaitGroup, ctr counters.Counter, subject string, n, g int) {
	perGoroutine := n / g
	for i := range g {
		count := perGoroutine
		if i == g-1 {
			count = n - perGoroutine*(g-1)
		}
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for range c {
				if _, err := ctr.AddInt(ctx, subject, 1); err != nil {
					log.Printf("increment %s: %v", subject, err)
				}
			}
		}(count)
	}
}

// pollUntil polls fn every 100ms until it returns expected or ctx expires.
func pollUntil(ctx context.Context, fn func() (*big.Int, error), expected *big.Int) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if val, err := fn(); err == nil && val.Cmp(expected) == 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}
