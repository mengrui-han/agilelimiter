package inbound

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Yiming1997/agilelimiter"
	iplimiter "github.com/Yiming1997/agilelimiter/Inbound-Rate-Limiter"
	httplimit "github.com/Yiming1997/agilelimiter/Inbound-Rate-Limiter/httplimiter"
	"github.com/Yiming1997/agilelimiter/Inbound-Rate-Limiter/memorystore"
	"github.com/Yiming1997/agilelimiter/httpserver"
)

// Handler combines optional IP quotas with adaptive concurrency admission.
type Handler struct {
	handler http.Handler
	store   iplimiter.Store
}

// NewHandler Create and assemble processing links
func NewHandler(
	next http.Handler,
	adaptive *agilelimiter.ConcurrencyLimiter,
	cfg Config,
) (*Handler, error) {
	if next == nil {
		return nil, fmt.Errorf("inbound: handler cannot be nil")
	}
	if adaptive == nil {
		return nil, fmt.Errorf("inbound: adaptive limiter cannot be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Reuse the existing adaptive admission and feedback implementation.
	// Return to a new business handle
	adaptiveHandler := httpserver.Wrap(
		next,
		adaptive,
		cfg.AdaptiveOptions...,
	)

	if !cfg.Enabled {
		return &Handler{handler: adaptiveHandler}, nil
	}

	// Retain buckets for at least one configured window.
	retention := 12 * time.Hour
	if cfg.Interval > retention {
		retention = cfg.Interval
	}

	store := memorystore.New(&memorystore.Config{
		Tokens:      cfg.Tokens,
		Interval:    cfg.Interval,
		SweepMinTTL: retention,
	})
	// Optional
	keyFunc := cfg.KeyFunc
	if keyFunc == nil {
		keyFunc = httplimit.IPKeyFunc()
	}

	ip, err := httplimit.NewMiddleware(store, keyFunc)
	if err != nil {
		_ = store.Close(context.Background())
		return nil, err
	}

	return &Handler{
		handler: ip.Handle(adaptiveHandler),
		store:   store,
	}, nil
}

// ServeHTTP delegates to the composed middleware link.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handler.ServeHTTP(w, r)
}

// Close releases the IP store owned by this handler.
// Stop serving requests before calling Close.
func (h *Handler) Close(ctx context.Context) error {
	if h.store == nil {
		return nil
	}
	return h.store.Close(ctx)
}
