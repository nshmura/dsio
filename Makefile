VERSION ?= $(shell git describe --tags)

build:
	go build -o dsio -ldflags "-X main.version=${VERSION}" dsio.go

init:
	go get golang.org/x/tools/cmd/goyacc
	go get -u github.com/golang/lint/golint

fmt:
	go fmt $$(glide novendor)

test:
	go test $$(glide novendor)

lint:
	golint $$(glide novendor)

yacc:
	cd gql; goyacc -o parser.go parser.go.y

yacc-test:
	go test ./gql/... -v

.PHONY: build init fmt test yacc yacc-test
