# Makefile for redis-fatigue
# Note: keep code comments in English per project preference.

# Image settings
REGISTRY ?=
IMG_NAME ?= redis-fatigue
TAG      ?= latest
IMG      := $(if $(REGISTRY),$(REGISTRY)/)$(IMG_NAME):$(TAG)

# Kubernetes settings
NAMESPACE ?= default
KUBECTL   ?= kubectl

# Build settings
GOOS        ?= linux
GOARCH      ?= amd64
CGO_ENABLED ?= 0

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build           - Build local binary (./redis-fatigue)"
	@echo "  docker-build    - Build docker image $(IMG)"
	@echo "  docker-push     - Push docker image $(IMG)"
	@echo "  k8s-apply       - Apply k8s manifests (ConfigMap + Deployment)"
	@echo "  k8s-delete      - Delete k8s resources"
	@echo "  k8s-set-image   - Update deployment image to $(IMG)"
	@echo "  k8s-status      - Show rollout status"
	@echo "  k8s-logs        - Tail deployment logs"
	@echo "  k8s-restart     - Restart deployment"

.PHONY: build
build:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) \
		go build -mod=vendor -trimpath -ldflags='-s -w' -o redis-fatigue ./main.go

.PHONY: docker-build
docker-build:
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push:
	@if [ -z "$(REGISTRY)" ]; then echo "REGISTRY is empty. Set REGISTRY to push (e.g. REGISTRY=registry.example.com)"; exit 1; fi
	docker push $(IMG)

.PHONY: k8s-apply
k8s-apply:
	$(KUBECTL) apply -n $(NAMESPACE) -f k8s/deployment.yaml

.PHONY: k8s-delete
k8s-delete:
	$(KUBECTL) delete -n $(NAMESPACE) -f k8s/deployment.yaml --ignore-not-found

.PHONY: k8s-set-image
k8s-set-image:
	$(KUBECTL) set image deployment/redis-fatigue redis-fatigue=$(IMG) -n $(NAMESPACE)

.PHONY: k8s-status
k8s-status:
	$(KUBECTL) rollout status deployment/redis-fatigue -n $(NAMESPACE)

.PHONY: k8s-logs
k8s-logs:
	$(KUBECTL) logs -f -n $(NAMESPACE) deploy/redis-fatigue --tail=200

.PHONY: k8s-restart
k8s-restart:
	$(KUBECTL) rollout restart deployment/redis-fatigue -n $(NAMESPACE)
