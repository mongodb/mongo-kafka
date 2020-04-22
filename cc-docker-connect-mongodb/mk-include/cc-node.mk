DOCKER_BUILD_PRE  += .npmrc
DOCKER_BUILD_POST += clean-npmrc
RELEASE_PRECOMMIT += set-node-bumped-version

NPM_REGISTRY ?= https://nexus.confluent.io/repository/npm-internal/
NPM_SCOPE ?= @confluent

.PHONY: set-node-bumped-version
set-node-bumped-version:
	test -f package.json \
		&& (npm version $(BUMPED_VERSION) --git-tag-version=false &&\
			git add package.json) \
		|| true

.npmrc: $(HOME)/.npmrc
	cp $(HOME)/.npmrc .npmrc

clean-npmrc:
	rm .npmrc

$(HOME)/.npmrc:
ifneq ($(NPM_USER)$(NPM_PASS)$(NPM_EMAIL),$(_empty))
	@docker run \
			-e NPM_USER=$(NPM_USER) \
			-e NPM_PASS=$(NPM_PASS) \
			-e NPM_EMAIL=$(NPM_EMAIL) \
			-e NPM_REGISTRY=$(NPM_REGISTRY) \
			-e NPM_SCOPE=$(NPM_SCOPE) \
			bravissimolabs/generate-npm-authtoken \
			> $(HOME)/.npmrc
else ifneq ("$(wildcard $(HOME)/.m2/settings.xml)","")
	@$(eval NPM_USER := $(shell xpath ~/.m2/settings.xml '//settings/servers[1]/server/username/text()' 2>/dev/null)) \
	 $(eval NPM_PASS := $(shell xpath ~/.m2/settings.xml '//settings/servers[1]/server/password/text()' 2>/dev/null)) \
	 $(eval NPM_EMAIL := $(NPM_USER)@confluent.io) \
	 docker run \
			-e NPM_USER=$(NPM_USER) \
			-e NPM_PASS=$(NPM_PASS) \
			-e NPM_EMAIL=$(NPM_EMAIL) \
			-e NPM_REGISTRY=$(NPM_REGISTRY) \
			-e NPM_SCOPE=$(NPM_SCOPE) \
			bravissimolabs/generate-npm-authtoken \
			> $(HOME)/.npmrc
else
	@echo "https://nexus.confluent.io not logged in, Username, Password, and Email not found in environment, prompting for login:" && \
		npm login --registry=https://nexus.confluent.io/repository/npm-internal/ --scope=@confluent
endif

.PHONY: npm-login
## Login to Confluent's private npm on Nexus
npm-login: $(HOME)/.npmrc
