VERSION ?= $(shell git describe --tags)

build:
	go build -o dsio -ldflags "-X main.version=${VERSION}" dsio.go

init:
	go get golang.org/x/tools/cmd/goyacc
	go get -u golang.org/x/lint

fmt:
	go fmt ./...

test:
	go test ./...

lint:
	golint ./...

yacc:
	cd gql; goyacc -o parser.go parser.go.y

yacc-test:
	go test ./gql/... -v

.PHONY: build init fmt test yacc yacc-test
