all: build

build:
	go build -o bin/terraform-provider-ydb main.go
