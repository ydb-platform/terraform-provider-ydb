SEMVER ?= 0.0.1

all: local-build

# local-build:
# 	go build -o terraform-provider-ydb main.go

local-build:
	go build -o $(HOME)/.terraform.d/plugins/terraform.storage.ydb.tech/provider/ydb/$(SEMVER)/$(shell go env GOOS)_$(shell go env GOARCH)/terraform-provider-ydb main.go
