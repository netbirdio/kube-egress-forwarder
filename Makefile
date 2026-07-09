IMG_REGISTRY ?= ghcr.io
IMG_REPOSITORY ?= netbirdio/kube-egress-forwarder
IMG_TAG ?= dev
IMG_REF := $(IMG_REGISTRY)/$(IMG_REPOSITORY):$(IMG_TAG)

.PHONY: lint
lint:
	@golangci-lint run ./...

.PHONY: test
test-unit:
	@go test ./... -race -coverprofile=coverage.txt

.PHONY: build
build: bin/linux-$(shell go env GOARCH)/kube-egress-forwarder

bin/linux-%/kube-egress-forwarder: $(shell find pkg) main.go go.mod go.sum
	@CGO_ENABLED=0 GOOS=linux GOARCH=$* go build -ldflags="-w -s" -trimpath -o $@ main.go

.PHONY: build-image
build-image: build
	@DOCKER_BUILDKIT=1 docker build -t ${IMG_REF} .
	@echo ${IMG_REF}

.PHONY: build-image-multiarch
build-image-multiarch: bin/linux-amd64/kube-egress-forwarder bin/linux-arm64/kube-egress-forwarder
	@DOCKER_BUILDKIT=1 docker build --platform linux/amd64,linux/arm64 -t ${IMG_REF} .
	@echo ${IMG_REF}
