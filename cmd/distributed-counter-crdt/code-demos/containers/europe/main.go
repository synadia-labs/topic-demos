package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/starfederation/datastar-go/datastar"
)

//go:embed static
var staticFiles embed.FS

const (
	signalName = "europeHits"
	subject    = "count.europe.hits"
)

type app struct {
	js     jetstream.JetStream
	stream jetstream.Stream
}

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	var nc *nats.Conn
	var err error
	for {
		nc, err = nats.Connect(natsURL)
		if err == nil {
			break
		}
		log.Printf("connect (retrying): %v", err)
		time.Sleep(time.Second)
	}
	defer nc.Close()
	log.Printf("connected: %s", natsURL)

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	cfg := jetstream.StreamConfig{
		Name:            "COUNTER_EUROPE",
		Subjects:        []string{subject},
		AllowMsgCounter: true,
		AllowDirect:     true,
		Storage:         jetstream.FileStorage,
		Sources: []*jetstream.StreamSource{
			{
				Name:   "COUNTER_SPAIN",
				Domain: "spain",
				SubjectTransforms: []jetstream.SubjectTransformConfig{{
					Source:      "count.spain.hits",
					Destination: subject,
				}},
			},
			{
				Name:   "COUNTER_FRANCE",
				Domain: "france",
				SubjectTransforms: []jetstream.SubjectTransformConfig{{
					Source:      "count.france.hits",
					Destination: subject,
				}},
			},
			{
				Name:   "COUNTER_ENGLAND",
				Domain: "england",
				SubjectTransforms: []jetstream.SubjectTransformConfig{{
					Source:      "count.england.hits",
					Destination: subject,
				}},
			},
		},
	}
	var stream jetstream.Stream
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err = js.CreateOrUpdateStream(ctx, cfg)
		cancel()
		if err == nil {
			break
		}
		log.Printf("create stream (retrying): %v", err)
		time.Sleep(time.Second)
	}
	log.Println("stream COUNTER_EUROPE ready")

	a := &app{js: js, stream: stream}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		data, _ := staticFiles.ReadFile("static/index.html")
		w.Write(data)
	})
	mux.HandleFunc("GET /counters", a.handleCounters)
	mux.HandleFunc("POST /hit/{node}", a.handleIncrement(1))
	mux.HandleFunc("POST /decrement/{node}", a.handleIncrement(-1))

	log.Println("listening :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func (a *app) readVal() string {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	msg, err := a.stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return "0"
	}
	var v struct {
		Val string `json:"val"`
	}
	if err := json.Unmarshal(msg.Data, &v); err != nil || v.Val == "" {
		return "0"
	}
	return v.Val
}

func (a *app) handleCounters(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	val := a.readVal()
	_ = sse.MarshalAndPatchSignals(map[string]any{signalName: val})
	prev := val

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			newVal := a.readVal()
			if newVal == prev {
				continue
			}
			prev = newVal
			_ = sse.MarshalAndPatchSignals(map[string]any{signalName: newVal})
		}
	}
}

func (a *app) handleIncrement(delta int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()

		if delta < 0 && a.readVal() == "0" {
			return
		}

		ack, err := a.js.PublishMsg(ctx, &nats.Msg{
			Subject: subject,
			Header:  nats.Header{"Nats-Incr": {fmt.Sprintf("%+d", delta)}},
		})
		if err != nil {
			log.Printf("increment: %v", err)
			http.Error(w, "increment failed", http.StatusInternalServerError)
			return
		}

		log.Printf("hit: node=%s delta=%d val=%s", r.PathValue("node"), delta, ack.Value)
		sse := datastar.NewSSE(w, r)
		_ = sse.MarshalAndPatchSignals(map[string]any{signalName: ack.Value})
	}
}
