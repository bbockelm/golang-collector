module github.com/bbockelm/golang-collector

go 1.25.7

require (
	github.com/PelicanPlatform/classad v0.1.0
	github.com/PelicanPlatform/classad/collections v0.0.0
	github.com/bbockelm/cedar v0.1.2
	github.com/bbockelm/golang-ccb v0.0.0-00010101000000-000000000000
	github.com/bbockelm/golang-htcondor v0.2.1
	github.com/prometheus/client_golang v1.13.0
)

require (
	github.com/RoaringBitmap/roaring/v2 v2.19.0 // indirect
	github.com/bbockelm/gosssd v0.0.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/time v0.14.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// The collector is built on the in-progress large-collection overhaul in the
// local classad checkout: the classad package (parent module) and the separate
// collections module (collections/ and collections/vm). cedar provides the
// CEDAR wire/security/command-dispatch layer; golang-htcondor provides the
// collector client used in round-trip tests.
replace (
	github.com/PelicanPlatform/classad => /Users/bbockelm/projects/golang-classads
	github.com/PelicanPlatform/classad/collections => /Users/bbockelm/projects/golang-classads/collections
	github.com/bbockelm/cedar => /Users/bbockelm/projects/golang-cedar
	github.com/bbockelm/golang-ccb => /Users/bbockelm/projects/golang-ccb
	github.com/bbockelm/golang-htcondor => /Users/bbockelm/projects/golang-htcondor
)
