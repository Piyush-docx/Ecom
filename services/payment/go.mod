module services/payment

go 1.25.0

replace pkg => ../../pkg

require (
	github.com/go-chi/chi/v5 v5.3.1
	github.com/jackc/pgx/v5 v5.10.0
	pkg v0.0.0-00010101000000-000000000000
)

require (
	github.com/golang-migrate/migrate/v4 v4.19.1 // indirect
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.16 // indirect
	github.com/segmentio/kafka-go v0.4.51 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/text v0.31.0 // indirect
)
