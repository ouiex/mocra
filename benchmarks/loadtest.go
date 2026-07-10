// Measures the mock server's raw ceiling with a fast concurrent Go client, to confirm the mock
// is never the bottleneck. Run:  go run loadtest.go
package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var count int64
	var wg sync.WaitGroup
	dur := 3 * time.Second
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: 500, MaxIdleConnsPerHost: 500}}
	stop := time.Now().Add(dur)
	for g := 0; g < 200; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			i := base
			for time.Now().Before(stop) {
				r, err := client.Get(fmt.Sprintf("http://127.0.0.1:8899/item/%d", i))
				if err == nil {
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
					atomic.AddInt64(&count, 1)
				}
				i += 200
			}
		}(g)
	}
	wg.Wait()
	fmt.Printf("mock ceiling: %d req in %.0fs = %.0f req/s (200 conns)\n", count, dur.Seconds(), float64(count)/dur.Seconds())
}
