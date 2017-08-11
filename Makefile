init:
	go get golang.org/x/tools/cmd/goyacc

yacc:
	goyacc -o parser.go parser.go.y

yacc-test:
	go test ./gql/... -v

fmt:
	go fmt $$(glide novendor)

test:
	go test $$(glide novendor)
