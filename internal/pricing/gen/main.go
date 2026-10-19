// Command gen regenerates the builtin price list from the public feed.
//
//	go generate ./internal/pricing
//	go run ./internal/pricing/gen -in instances.json -out builtin.json.gz
package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/s3cretagent/kubed/internal/pricing"
)

func main() {
	in := flag.String("in", "", "read the feed from a local file instead of downloading it")
	out := flag.String("out", "builtin.json.gz", "output path")
	flag.Parse()

	t, err := load(*in)
	if err != nil {
		log.Fatal(err)
	}
	if err := write(*out, t); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s: %d regions", *out, len(t.Regions))
}

func load(path string) (*pricing.Table, error) {
	if path == "" {
		return pricing.Fetch(context.Background(), &http.Client{Timeout: 5 * time.Minute})
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return pricing.ParseFeed(f, st.ModTime().UTC())
}

func write(path string, t *pricing.Table) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	zw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		f.Close()
		return err
	}
	if err := json.NewEncoder(zw).Encode(t); err != nil {
		f.Close()
		return err
	}
	if err := zw.Close(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
