SEMVER ?= 0.0.1

# Acceptance tests (Terraform CLI on PATH; TF_ACC=1). Optional YDB_ACC_TOKEN / YDB_ACC_USER / YDB_ACC_PASSWORD.
# Default connection matches local YDB (see internal/terraform/acc_test.go).
YDB_ACC_CONNECTION_STRING ?= grpc://127.0.0.1:2136/?database=/local
ACC_TEST_TIMEOUT         ?= 30m
ACC_TEST_PKG             ?= ./internal/terraform/

all: local-build

# local-build:
# 	go build -o terraform-provider-ydb main.go

local-build:
	go build -o $(HOME)/.terraform.d/plugins/terraform.storage.ydb.tech/provider/ydb/$(SEMVER)/$(shell go env GOOS)_$(shell go env GOARCH)/terraform-provider-ydb main.go

build:
	go build -o bin/terraform-provider-ydb main.go

.PHONY: test-acc
test-acc:
	TF_ACC=1 YDB_ACC_CONNECTION_STRING='$(YDB_ACC_CONNECTION_STRING)' go test -v $(ACC_TEST_PKG) -run TestAcc -timeout $(ACC_TEST_TIMEOUT)
