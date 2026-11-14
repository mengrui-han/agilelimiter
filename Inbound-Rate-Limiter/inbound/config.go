package inbound

import (
	"fmt"
	"time"

	httplimit "github.com/Yiming1997/agilelimiter/Inbound-Rate-Limiter/httplimiter"
	"github.com/Yiming1997/agilelimiter/httpserver"
)

// Config configures IP quotas and adaptive HTTP admission.
type Config struct {
	// Enabled controls the IP quota layer.
	// Adaptive concurrency admission remains enabled.
	Enabled bool

	Tokens   uint64
	Interval time.Duration

	// KeyFunc defaults to IPKeyFunc when nil.
	KeyFunc httplimit.KeyFunc

	// AdaptiveOptions configure the existing concurrency middleware.
	AdaptiveOptions []httpserver.Option
}

// Validate checks the enabled IP quota configuration.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Tokens == 0 {
		return fmt.Errorf("inbound: tokens must be positive")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("inbound: interval must be positive")
	}
	return nil
}
