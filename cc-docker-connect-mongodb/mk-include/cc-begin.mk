# Set shell to bash
SHELL := /bin/bash

# Include this file first
_empty :=
_space := $(_empty) $(empty)

# Master branch
MASTER_BRANCH ?= master

RELEASE_TARGETS += $(_empty)
BUILD_TARGETS += $(_empty)
TEST_TARGETS += $(_empty)
CLEAN_TARGETS += $(_empty)

# If this variable is set, release will run make $(RELEASE_MAKE_TARGETS)
RELEASE_MAKE_TARGETS +=

ifeq ($(SEMAPHORE),true)
CI_BIN := $(SEMAPHORE_CACHE_DIR)/bin
else ifeq ($(BUILDKITE),true)
CI_BIN := /tmp/bin
endif

ifeq ($(BIN_PATH),)
ifeq ($(CI),true)
BIN_PATH := $(CI_BIN)
else
BIN_PATH ?= /usr/local/bin
endif
endif

# Git stuff
BRANCH_NAME ?= $(shell test -d .git && git rev-parse --abbrev-ref HEAD)
# Set RELEASE_BRANCH if we're on master or vN.N.x
RELEASE_BRANCH := $(shell echo $(BRANCH_NAME) | grep -E '^($(MASTER_BRANCH)|v[0-9]+\.[0-9]+\.x)$$')
# assume the remote name is origin by default
GIT_REMOTE_NAME ?= origin

# Determine if we're on a hotfix branch
ifeq ($(RELEASE_BRANCH),$(MASTER_BRANCH))
HOTFIX := false
else
HOTFIX := true
endif

ifeq ($(CI),true)
_ := $(shell test -d $(CI_BIN) || mkdir -p $(CI_BIN))
export PATH = $(CI_BIN):$(shell printenv PATH)
endif

.PHONY: update-mk-include
update-mk-include:
	git subtree pull --prefix mk-include git@github.com:confluentinc/cc-mk-include.git master --squash

.PHONY: add-github-templates
add-github-templates:
	$(eval project_root := $(shell git rev-parse --show-toplevel))
	$(eval mk_include_relative_path := ../mk-include)
	$(if $(wildcard $(project_root)/.github/pull_request_template.md),$(a error ".github/pull_request_template.md already exists, try deleting it"),)
	$(if $(filter $(BRANCH_NAME),$(MASTER_BRANCH)),$(error "You must run this command from a branch: 'git checkout -b add-github-pr-template'"),)

	@mkdir -p $(project_root)/.github
	@ln -s $(mk_include_relative_path)/.github/pull_request_template.md $(project_root)/.github
	@git add $(project_root)/.github/pull_request_template.md
	@git commit \
		-m "Add .github template for PRs $(CI_SKIP)" \
		-m "Adds the .github/pull_request_template.md as described in [1]" \
		-m "linking to the shared template in \`mk-include\`." \
		-m "" \
		-m "[1] https://github.com/confluentinc/cc-mk-include/pull/113"

	@git show
	@echo "Template added."
	@echo "Create PR with 'git push && git log --format=%B -n 1 | hub pull-request -F -'"

.PHONY: bats
bats:
	find . -name *.bats -exec bats {} \;

$(HOME)/.netrc:
	@echo .netrc missing, prompting for user input
	@echo Enter Github credentials, if you use 2 factor authentcation generate a personal access token for the password: https://github.com/settings/tokens
	$(eval user := $(shell bash -c 'read -p "GitHub Username: " user; echo $$user'))
	$(eval pass := $(shell bash -c 'read -s -p "GitHub Password: " pass; echo $$pass'))
	@printf "machine github.com\n\tlogin $(user)\n\tpassword $(pass)" > $(HOME)/.netrc

.netrc: $(HOME)/.netrc
	cp $(HOME)/.netrc .netrc

.ssh: $(HOME)/.ssh
	cp -R $(HOME)/.ssh .ssh
