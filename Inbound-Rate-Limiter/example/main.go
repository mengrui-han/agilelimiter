package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"github.com/Yiming1997/agilelimiter/Inbound-Rate-Limiter/inbound"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	adaptive := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		InitialLimit: 10,
		MaxLimit:     200,
		SLOTarget:    100 * time.Millisecond,
	})
	adaptive.Start()
	defer adaptive.Stop()

	work := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	// Create the shared middleware before starting the HTTP server.
	handler, err := inbound.NewHandler(work, adaptive, inbound.Config{
		Enabled:  true,
		Tokens:   3,
		Interval: 10 * time.Second,
	})
	if err != nil {
		return err
	}
	defer handler.Close(context.Background())

	server := &http.Server{
		Addr:              "127.0.0.1:8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	result := make(chan error, 1)
	go func() {
		result <- server.ListenAndServe()
	}()

	log.Printf("listening on http://%s; press Ctrl+C to stop", server.Addr)

	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err

	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return err
		}

		err := <-result
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
