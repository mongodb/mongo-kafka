SERVICE_NAME ?=
MAIN_GO ?= main.go
GO_OUTDIR ?= bin

# for terraform
MODULE_NAME ?= $(SERVICE_NAME)
# for docker images
IMAGE_NAME ?= $(SERVICE_NAME)
# for helm charts
CHART_NAME ?= $(SERVICE_NAME)

BASE_IMAGE ?= confluent-docker.jfrog.io/confluentinc/cc-service-base
BASE_VERSION ?= 1.9

GO_LDFLAGS ?= "-X main.version=$(VERSION)"

ALL_SRC = $(shell find . -type d -path ./vendor -prune -o -type d -path ./.gomodcache -prune -o -type d -path ./.semaphore-cache -prune -o -name \*.go -not -name bindata.go -print)

CODECOV ?= false
GO_TEST_ARGS ?= -race -v -cover
GO_CODECOV_TEST_ARGS ?= -race -v

GO111MODULE ?= off
export GO111MODULE

DEP_VERSION ?= v0.5.0
ifeq ($(CI),true)
DEP_ARGS := -vendor-only

# Override GOPATH so that mods get cached
ifeq ($(GO111MODULE),on)
GOPATH := $(SEMAPHORE_CACHE_DIR)/go
export GOPATH
endif

endif

GOPATH ?= $(shell go env GOPATH)

GO_BUILD_TARGET ?= build-go
GO_TEST_TARGET ?= lint-go test-go
GO_CLEAN_TARGET ?= clean-go

INIT_CI_TARGETS += deps
BUILD_TARGETS += $(GO_BUILD_TARGET)
TEST_TARGETS += $(GO_TEST_TARGET)
CLEAN_TARGETS += $(GO_CLEAN_TARGET)
RELEASE_POSTCOMMIT += $(GO_DOWNSTREAM_DEPS)
DOCKER_BUILD_PRE += .gomodcache

GO_BINDATA_VERSION := 3.11.0
GO_BINDATA_OPTIONS ?=

ifeq ($(CI),true)
# Override the DB_URL for go tests that need access to postgres
DB_URL ?= postgres://$(DATABASE_POSTGRESQL_USERNAME):$(DATABASE_POSTGRESQL_PASSWORD)@127.0.0.1:5432/mothership?sslmode=disable
export DB_URL
endif

.PHONY: show-go
## Show Go Variables
show-go:
	@echo "SERVICE_NAME: $(SERVICE_NAME)"
	@echo "MAIN_GO: $(MAIN_GO)"
	@echo "GO_OUTDIR: $(GO_OUTDIR)"
	@echo "GO_LDFLAGS: $(GO_LDFLAGS)"
	@echo "DEP_ARGS: $(DEP_ARGS)"
	@echo "IMAGE_NAME: $(IMAGE_NAME)"
	@echo "CHART_NAME: $(CHART_NAME)"
	@echo "MODULE_NAME: $(MODULE_NAME)"
	@echo "GO111MODULE: $(GO111MODULE)"
	@echo "GO_BINDATA_VERSION: $(GO_BINDATA_VERSION)"
	@echo "DB_URL: $(DB_URL)"
	@echo "GO_DOWNSTREAM_DEPS: $(GO_DOWNSTREAM_DEPS)"

.PHONY: clean-go
clean-go:
	rm -rf $(SERVICE_NAME) .netrc bin/ .gomodcache/

.PHONY: vet
vet:
	@go list ./... | grep -v vendor | xargs go vet

.PHONY: deps
## Install and run dep ensure (with -vendor-only if on CI) or run go mod download
deps: $(HOME)/.hgrc $(GO_EXTRA_DEPS)
ifeq ($(GO111MODULE),off)
	@(dep version | grep $(DEP_VERSION)) || (mkdir -p $(GOPATH)/bin && DEP_RELEASE_TAG=$(DEP_VERSION) curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh && dep version)
	dep ensure $(DEP_ARGS)
else
	@test -f Gopkg.toml && echo "WARNING: GO111MODULE enabled but Gopkg.toml found!" || true
	@test -f go.mod || echo "ERROR: GO111MODULE enabled but go.mod not found!"
	@test -f go.sum || echo "ERROR: GO111MODULE enabled but go.sum not found!"
	go mod download
	go mod verify
endif

$(HOME)/.hgrc:
	echo -e '[ui]\ntls = False' > $@

.PHONY: .gomodcache
.gomodcache:
ifeq ($(GO111MODULE),on)
	rm -rf .gomodcache; cp -r $(GOPATH)/pkg/mod/cache/download .gomodcache
endif

.PHONY: lint-go
## Lints (gofmt)
lint-go: $(GO_EXTRA_LINT)
	@test -z "$$(gofmt -e -s -l -d $(ALL_SRC) | tee /dev/tty)"

.PHONY: fmt
# Format entire codebase
fmt:
	@gofmt -e -s -l -w $(ALL_SRC)

.PHONY: build-go
## Build just the go project, override with BUILD_GO_OVERRIDE
build-go: go-bindata $(BUILD_GO_OVERRIDE)
ifeq ($(BUILD_GO_OVERRIDE),)
	go build -o $(GO_OUTDIR)/$(SERVICE_NAME) -ldflags $(GO_LDFLAGS) $(MAIN_GO)
endif

.PHONY: install
## Install the go binary into $GOBIN
install:
	go build -o $(GOBIN)/$(SERVICE_NAME) $(MAIN_GO)

.PHONY: run
## Run MAIN_GO
run: deps
	go run $(MAIN_GO)

.PHONY: test-go
## Run Go Tests and Vet code
test-go: vet
ifeq ($(CI)$(CODECOV),truetrue)
	test -f coverage.txt && truncate -s 0 coverage.txt || true
	go test $(GO_CODECOV_TEST_ARGS) -coverprofile=coverage.txt ./...
	curl -s https://codecov.io/bash | bash
else
	go test $(GO_TEST_ARGS) ./...
endif

.PHONY: generate
## Run go generate
generate:
	go generate

SEED_POSTGRES_URL ?= postgres://

.PHONY: seed-local-mothership
## Seed local mothership DB. Optionally set SEED_POSTGRES_URL for base postgres url
seed-local-mothership:
	@echo "Seeding postgres in 'SEED_POSTGRES_URL=${SEED_POSTGRES_URL}'. Set SEED_POSTGRES_URL to override"
	psql ${SEED_POSTGRES_URL}/postgres -c 'DROP DATABASE IF EXISTS mothership;'
	psql ${SEED_POSTGRES_URL}/postgres -c 'CREATE DATABASE mothership;'
	psql ${SEED_POSTGRES_URL}/mothership -f mk-include/seed-db/mothership-seed.sql

.PHONY: install-go-bindata
GO_BINDATA_INSTALLED_VERSION := $(shell $(BIN_PATH)/go-bindata -version 2>/dev/null | head -n 1 | awk '{print $$2}' | xargs)
install-go-bindata:
	@echo "go-bindata installed version: $(GO_BINDATA_INSTALLED_VERSION)"
	@echo "go-bindata want version: $(GO_BINDATA_VERSION)"
ifneq ($(GO_BINDATA_INSTALLED_VERSION),$(GO_BINDATA_VERSION))
	curl -L -o $(BIN_PATH)/go-bindata https://github.com/kevinburke/go-bindata/releases/download/v$(GO_BINDATA_VERSION)/go-bindata-$(shell go env GOOS)-$(shell go env GOARCH)
	chmod +x $(BIN_PATH)/go-bindata
endif

.PHONY: go-bindata
ifneq ($(GO_BINDATA_OPTIONS),)
## Run go-bindata for project
go-bindata: install-go-bindata
	go-bindata $(GO_BINDATA_OPTIONS)
ifeq ($(CI),true)
ifeq ($(findstring pull-request,$(BRANCH_NAME)),pull-request)
	git diff --exit-code --name-status || \
		(echo "ERROR: cannot commit changes back to a fork, please run go-bindata locally and commit the changes" && exit 1)
else
	git diff --exit-code --name-status || \
		(git add deploy/bindata.go && \
		git commit -m 'chore: updating bindata' && \
		git push $(GIT_REMOTE_NAME) $(BRANCH_NAME))
endif
endif
else
go-bindata:
endif

.PHONY: go-update-deps
## Update dependencies (go get -u)
go-update-deps:
ifeq ($(HOTFIX),true)
	go get -u=patch
else
	go get -u
endif
	go mod tidy

.PHONY: go-update-dep
## Update single dependency, specify with DEP=
go-update-dep:
ifeq ($(DEP),)
	@echo "Error: must specify DEP= on the commandline"
	@echo "Usage: make go-update-dep DEP=github.com/confluentinc/example@v1.2.3"
else
	go get $(DEP)
	go mod tidy
endif

.PHONY: go-commit-deps
## Commit (and push) updated go deps.
## NOTE: Some repos have go.sum in `.gitignore`. We use `git ls-files` to only
## add go.sum if it's tracked by git.
go-commit-deps:
	git diff --exit-code --name-status || \
		(git add $(git ls-files go.mod go.sum) && \
		git commit -m 'chore: $(UPSTREAM_MOD):$(UPSTREAM_VERSION) updating go deps' && \
		git push $(GIT_REMOTE_NAME) $(GIT_BRANCH_NAME))

.PHONY: $(GO_DOWNSTREAM_DEPS)
$(GO_DOWNSTREAM_DEPS):
ifeq ($(HOTFIX),true)
	@echo "Skipping bumping downstream go dep $@ on hotfix branch"
else ifeq ($(BUMP),major)
	@echo "Skipping bumping downstream go dep $@ with major version bump"
else
	git clone git@github.com:confluentinc/$@.git $@
	make -C $@ go-update-dep go-commit-deps \
		DEP=$(shell grep module go.mod | awk '{print $$2}')@$(BUMPED_VERSION) \
		UPSTREAM_MOD=$(SERVICE_NAME) \
		UPSTREAM_VERSION=$(BUMPED_VERSION)
	rm -rf $@
endif
