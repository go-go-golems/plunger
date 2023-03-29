.PHONY: gifs

all: gifs

TAPES=$(shell ls doc/vhs/*tape)
gifs: $(TAPES)
	for i in $(TAPES); do vhs < $$i; done

docker-lint:
	docker run --rm -v $(shell pwd):/app -w /app golangci/golangci-lint:v1.50.1 golangci-lint run -v

lint:
	golangci-lint run -v --enable=exhaustive

test:
	go test ./...

build:
	go generate ./...
	go build ./...

goreleaser:
	goreleaser release --skip-sign --snapshot --rm-dist

tag-major:
	git tag $(shell svu major)

tag-minor:
	git tag $(shell svu minor)

tag-patch:
	git tag $(shell svu patch)

release:
	git push --tags
	GOPROXY=proxy.golang.org go list -m github.com/go-go-golems/plunger@$(shell svu current)

exhaustive:
	golangci-lint run -v --enable=exhaustive

bump-glazed:
	go get github.com/go-go-golems/glazed@latest
	go mod tidy


PLUNGER_BINARY=$(shell which plunger)

install:
	go build -o ./dist/plunger ./cmd/plunger && \
		cp ./dist/plunger $(PLUNGER_BINARY)
