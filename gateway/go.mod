module gateway

go 1.24

require (
	github.com/go-chi/chi/v5 v5.3.1
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/redis/go-redis/v9 v9.21.0
	ratelimiter v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

// ratelimiter is a sibling module in this workspace, not a published one.
// go.work covers local builds; this replace keeps `go build` working inside
// the module directory alone (CI, docker builds) where the workspace is absent.
replace ratelimiter => ../ratelimiter
