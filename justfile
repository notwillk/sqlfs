build:
  @PATH="$(go env GOPATH)/bin:$PATH" goreleaser release --clean --snapshot --skip=publish --skip=announce

compile:
  @mkdir -p dist
  @cd src && go build -o ../dist/sqlfs ./cmd/sqlfs
  @echo "Rebuilt"

dev:
  @echo "Watching src for changes. Press Ctrl+C to stop."
  @find src -type f | entr -r sh -c 'just compile'

release version:
  @./scripts/release.sh {{version}}

test:
  @cd src && go test ./...
