// Fast, multi-threaded mock crawl target so the CRAWLER (not the server) is the bottleneck.
//
//	GET /        -> catalog page linking to /item/0 .. /item/{N-1}
//	GET /item/ID -> a small HTML page with fields to extract (title / price / desc)
//	GET /health  -> "ok"
//
// Run:  N=5000 go run mock.go   (listens on :8899; ADDR overrides)
package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func main() {
	n := 5000
	if v := os.Getenv("N"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	addr := ":8899"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}

	var b strings.Builder
	b.WriteString("<html><body><h1>catalog</h1>")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `<a class="item" href="/item/%d">item %d</a>`, i, i)
	}
	b.WriteString("</body></html>")
	seed := b.String()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(seed))
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/item/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/item/")
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<html><body><h1 class="title">Item %s</h1><span class="price">%s.99</span><p class="desc">Some description text for item %s so the parser has real work to do.</p></body></html>`, id, id, id)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })

	fmt.Printf("mock: listening on %s, N=%d items\n", addr, n)
	if err := (&http.Server{Addr: addr}).ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
