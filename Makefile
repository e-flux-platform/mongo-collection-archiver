DOCKER_REPOSITORY ?= quay.io/road
BRANCH_NAME ?= $(shell git rev-parse --abbrev-ref HEAD)
GIT_HASH ?= $(shell git rev-parse HEAD)
RELEASE_ID ?= $(shell id -un)-local
DOCKER_LATEST_TAG ?= latest

build:
	docker buildx build \
	--cache-from type=local,src=/tmp/.buildx-$(BRANCH_NAME)-cache \
	--cache-to type=local,mode=max,dest=/tmp/.buildx-$(BRANCH_NAME)-cache \
	--provenance mode=min,inline-only=true \
	--tag $(DOCKER_REPOSITORY)/mongo-collection-archiver:$(GIT_HASH) \
	--tag $(DOCKER_REPOSITORY)/mongo-collection-archiver:$(DOCKER_LATEST_TAG) \
	--tag $(DOCKER_REPOSITORY)/mongo-collection-archiver:release-$(RELEASE_ID) \
	--push \
	.

push: build
