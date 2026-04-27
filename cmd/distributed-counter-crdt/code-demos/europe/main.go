package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"text/template"
	"time"

	natssrv "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/starfederation/datastar-go/datastar"
)

//go:embed static
var staticFiles embed.FS

const (
	signalName = "hits"
	subject    = "count.europe.hits"
	streamName = "COUNTER_EUROPE"
)

var (
	nodeName  = getEnv("NODE_NAME", "europe")
	nodeColor = getEnv("NODE_COLOR", "#22c55e")
	nodeTitle = getEnv("NODE_TITLE", "Europe")
)

type navLink struct {
	Label string
	URL   string
}

type pageConfig struct {
	NodeName string
	Color    string
	Title    string
	NavLinks []navLink
	ReadOnly bool
}

var navLinks = []navLink{
	{Label: "↑ Global", URL: "http://localhost:8081"},
	{Label: "Spain", URL: "http://localhost:8084"},
	{Label: "France", URL: "http://localhost:8085"},
	{Label: "England", URL: "http://localhost:8086"},
}

type app struct {
	js     jetstream.JetStream
	stream jetstream.Stream
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	storeDir := os.Getenv("STORE_DIR")
	if storeDir == "" {
		storeDir = "/data"
	}

	leafRemote := os.Getenv("LEAF_REMOTE_URL")
	leafNode := natssrv.LeafNodeOpts{Port: 7422}
	if leafRemote != "" {
		u, err := url.Parse(leafRemote)
		if err != nil {
			log.Fatalf("parse LEAF_REMOTE_URL: %v", err)
		}
		leafNode.Remotes = []*natssrv.RemoteLeafOpts{{URLs: []*url.URL{u}}}
	}

	opts := &natssrv.Options{
		ServerName:      "europe",
		JetStream:       true,
		JetStreamDomain: "europe",
		StoreDir:        storeDir,
		HTTPPort:        8222,
		LeafNode:        leafNode,
	}

	ns, err := natssrv.NewServer(opts)
	if err != nil {
		log.Fatalf("nats server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		log.Fatal("nats server not ready after 10s")
	}
	log.Printf("embedded nats server ready: %s", ns.ClientURL())

	var nc *nats.Conn
	for {
		nc, err = nats.Connect(ns.ClientURL())
		if err == nil {
			break
		}
		log.Printf("connect (retrying): %v", err)
		time.Sleep(time.Second)
	}
	defer nc.Close()

	if _, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		log.Printf("[msg] subject=%s incr=%s", msg.Subject, msg.Header.Get("Nats-Incr"))
	}); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

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

	subRegions := []struct {
		localStream  string
		sourceStream string
		domain       string
		subject      string
	}{
		{"COUNTER_FRANCE_VIEW", "COUNTER_FRANCE", "france", "count.france.hits"},
		{"COUNTER_SPAIN_VIEW", "COUNTER_SPAIN", "spain", "count.spain.hits"},
		{"COUNTER_ENGLAND_VIEW", "COUNTER_ENGLAND", "england", "count.england.hits"},
	}
	for _, sr := range subRegions {
		srcfg := jetstream.StreamConfig{
			Name:        sr.localStream,
			AllowDirect: true,
			Storage:     jetstream.FileStorage,
			Mirror: &jetstream.StreamSource{
				Name:   sr.sourceStream,
				Domain: sr.domain,
			},
		}
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = js.CreateOrUpdateStream(ctx, srcfg)
			cancel()
			if err == nil {
				break
			}
			log.Printf("create view stream %s (retrying): %v", sr.localStream, err)
			time.Sleep(time.Second)
		}
		if err != nil {
			log.Printf("view stream %s unavailable: %v", sr.localStream, err)
			continue
		}
		log.Printf("view stream %s ready", sr.localStream)
	}

	a := &app{js: js, stream: stream}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		data, _ := staticFiles.ReadFile("static/index.html")
		tmpl := template.Must(template.New("index").Parse(string(data)))
		w.Header().Set("Content-Type", "text/html")
		_ = tmpl.Execute(w, pageConfig{NodeName: nodeName, Color: nodeColor, Title: nodeTitle, NavLinks: navLinks, ReadOnly: true})
	})
	mux.HandleFunc("GET /counters", a.handleCounters)
	mux.HandleFunc("POST /hit/{node}/{amount}", a.handleHit)
	mux.HandleFunc("POST /zero/{node}", a.handleZero)

	addr := getEnv("HTTP_ADDR", ":8080")
	log.Printf("listening %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func valFromMsg(data []byte) string {
	var v struct {
		Val string `json:"val"`
	}
	if err := json.Unmarshal(data, &v); err != nil || v.Val == "" {
		return "0"
	}
	return v.Val
}

func (a *app) readVal() string {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	msg, err := a.stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return "0"
	}
	return valFromMsg(msg.Data)
}

func (a *app) handleCounters(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	ctx := r.Context()

	cons, err := a.js.OrderedConsumer(ctx, streamName, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverLastPolicy,
	})
	if err != nil {
		return
	}
	iter, err := cons.Messages()
	if err != nil {
		return
	}
	defer iter.Stop()

	for {
		msg, err := iter.Next()
		if err != nil {
			return
		}
		msg.Ack()
		_ = sse.MarshalAndPatchSignals(map[string]any{signalName: valFromMsg(msg.Data())})
	}
}

func (a *app) handleHit(w http.ResponseWriter, r *http.Request) {
	amount, err := strconv.Atoi(r.PathValue("amount"))
	if err != nil || amount <= 0 {
		http.Error(w, "invalid amount", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	ack, err := a.js.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Header:  nats.Header{"Nats-Incr": {fmt.Sprintf("%+d", amount)}},
	})
	if err != nil {
		log.Printf("increment: %v", err)
		http.Error(w, "increment failed", http.StatusInternalServerError)
		return
	}
	log.Printf("hit: node=%s delta=%d val=%s", r.PathValue("node"), amount, ack.Value)
	sse := datastar.NewSSE(w, r)
	_ = sse.MarshalAndPatchSignals(map[string]any{signalName: ack.Value})
}

func (a *app) handleZero(w http.ResponseWriter, r *http.Request) {
	current, err := strconv.Atoi(a.readVal())
	if err != nil || current == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	ack, err := a.js.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Header:  nats.Header{"Nats-Incr": {fmt.Sprintf("%+d", -current)}},
	})
	if err != nil {
		log.Printf("zero: %v", err)
		http.Error(w, "zero failed", http.StatusInternalServerError)
		return
	}
	log.Printf("zero: node=%s delta=%d val=%s", r.PathValue("node"), -current, ack.Value)
	sse := datastar.NewSSE(w, r)
	_ = sse.MarshalAndPatchSignals(map[string]any{signalName: ack.Value})
}
