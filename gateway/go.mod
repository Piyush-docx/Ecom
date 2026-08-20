module gateway

go 1.25.0

require (
	github.com/go-chi/chi/v5 v5.3.1
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/redis/go-redis/v9 v9.21.0
	ratelimiter v0.0.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.24.1 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	pkg v0.0.0-00010101000000-000000000000
)

// ratelimiter is a sibling module in this workspace, not a published one.
// go.work covers local builds; this replace keeps `go build` working inside
// the module directory alone (CI, docker builds) where the workspace is absent.
replace ratelimiter => ../ratelimiter

replace pkg => ../pkg
