# Confluent Cloud Makefile Includes
This is a set of Makefile include targets that are used in cloud applications.

The purpose of cc-mk-include is to present a consistent developer experience across repos/projects:
```
make deps
make build
make test
make clean
```

It also helps standardize our CI pipeline across repos:
```
make init-ci
make build
make test
make release-ci
```

## Install
Add this repo to your repo with the command:
```shell
git subtree add --prefix mk-include git@github.com:confluentinc/cc-mk-include.git master --squash
```

Then update your makefile like so:

### Go + Docker + Helm Service
```make
SERVICE_NAME := cc-scraper
MAIN_GO := cmd/scraper/main.go
BASE_IMAGE := golang
BASE_VERSION := 1.9

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-go.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-end.mk
```

### Docker + Helm Only Service
```make
IMAGE_NAME := cc-example
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/caas-base-alpine
BASE_VERSION := v0.6.1

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-end.mk
```

## Updating
Once you have the make targets installed, you can update at any time by running

```shell
make update-mk-include
```

## Add github templates

To add the github PR templates to your repo

```shell
make add-github-templates
```

## Developing

If you're developing an app that uses cc-mk-include or needs to extend it, it's useful
to understand how the "library" is structured.

We expose a handful of extension points that are used internally and available for individual
apps as well. For example, when you include `cc-go.mk` it adds `clean-go` to `CLEAN_TARGETS`,
`build-go` to `BUILD_TARGETS`, and so on. Each of these imports (like `semver`, `go`, `docker`, etc)
is essentially a standardized extension.

**The ultimate effect is to be able to "mix and match" different extensions
(e.g., semver, docker, go, helm, cpd) for different applications.**

You can run `make show-args` when you're inside any given project to see what extensions
are enabled for a given standard extensible command. For example, we can see that when you
run `make build` in the `cc-scheduler-service`, it'll run `build-go`, `build-docker`,
`helm-set-version`, and `helm-package`.
```
cc-scheduler-service cody$ make show-args
INIT_CI_TARGETS:      seed-local-mothership deps cpd-update gcloud-install helm-setup-ci
CLEAN_TARGETS:         clean-go clean-images clean-terraform clean-cc-system-tests helm-clean
BUILD_TARGETS:         build-go build-docker helm-set-version helm-package
TEST_TARGETS:          lint-go test-go test-cc-system helm-lint
RELEASE_TARGETS:      set-tf-bumped-version helm-set-bumped-version helm-add-requirements get-release-image commit-release tag-release cc-cluster-spec-service push-docker cc-umbrella-chart
RELEASE_MAKE_TARGETS:  bump-downstream-tf-consumers helm-release
CI_BIN:
```

This also shows the full list of supported extension points (`INIT_CI_TARGETS`, `CLEAN_TARGETS`, and so on).

Applications themselves may also use these extension points; for example, you can append
your custom `clean-myapp` target to `CLEAN_TARGETS` to invoke as part of `make clean`.

We also expose a small number of override points for special cases (e.g., `BUILD_DOCKER_OVERRIDE`)
but these should be rather rare.
