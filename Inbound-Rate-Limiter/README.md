# Inbound IP rate limiter

Adapted from the local go-limiter project (Apache-2.0; see LICENSE.go-limiter).
Existing local English comments and the Store interface were preserved.
Changes include local imports, standard-library timestamps, and Go 1.20-compatible
window-boundary clamping. This directory uses the parent agilelimiter module.

## Layout

- store.go: shared Store interface and ErrStopped.
- memorystore/: concurrent, per-key fixed-window limiter.
- httplimiter/: HTTP middleware (Go package name: httplimit) and IP keys.
- noopstore/: always-allow backend for development.
- example/: IP middleware composed outside the adaptive concurrency middleware.

The IP middleware consumes quota for each attempt that passes its own gate,
including attempts later rejected by adaptive concurrency admission. IP rejection
does not enter the adaptive middleware or affect its latency/outage feedback.
State is process-local. IPKeyFunc() uses RemoteAddr; forwarded headers require a
trusted proxy policy. Fixed windows permit boundary bursts. The inherited cleanup
is best-effort and is not atomic with concurrent bucket updates.

Run from the agilelimiter repository root:

    go build ./Inbound-Rate-Limiter/...
    go run ./Inbound-Rate-Limiter/example

The example listens on 127.0.0.1:8080 with 3 requests per IP per 10-second window.
Its concurrency settings are demonstration values, not measured capacity limits.
During migration, overwritten files and the nested go.mod are backed up under
.reuse-backup/ with non-Go extensions.
