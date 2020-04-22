CHART_NAME ?=
IMAGE_VERSION ?= 0.0.0

HELM_VERSION := v2.9.1
HELM_URL := https://storage.googleapis.com/kubernetes-helm
HELM_TGZ := helm-$(HELM_VERSION)-linux-amd64.tar.gz

INIT_CI_TARGETS += helm-setup-ci
BUILD_TARGETS += helm-set-version helm-package
TEST_TARGETS += helm-lint
CLEAN_TARGETS += helm-clean
RELEASE_PRECOMMIT += helm-set-bumped-version helm-add-requirements
RELEASE_POSTCOMMIT += $(HELM_DOWNSTREAM_CHARTS)
RELEASE_MAKE_TARGETS += helm-release

CHART_VERSION := $(VERSION_NO_V)
BUMPED_CHART_VERSION := $(BUMPED_CLEAN_VERSION)

.PHONY: show-helm
## Show helm variables
show-helm:
	@echo "CHART_NAME: $(CHART_NAME)"
	@echo "IMAGE_VERSION: $(IMAGE_VERSION)"
	@echo "CHART_VERSION: $(VERSION_NO_V)"
	@echo "BUMPED_CHART_VERSION: $(BUMPED_CLEAN_VERSION)"
	@echo "HELM_DOWNSTREAM_CHARTS: $(HELM_DOWNSTREAM_CHARTS)"

.PHONY: helm-clean
helm-clean:
	helm delete $(CHART_NAME)-dev --purge

.PHONY: helm-lint
helm-lint:
	helm lint charts/$(CHART_NAME)

.PHONY: helm-deploy-local
## Deploy helm to current kube context with values set to local.yaml
helm-deploy-local:
	helm upgrade \
	--install $(CHART_NAME)-dev \
	charts/$(CHART_NAME) \
	--namespace $(CHART_NAME)-dev \
	--set namespace=$(CHART_NAME)-dev \
	--debug \
	-f charts/values/local.yaml \
	--set image.tag=$(IMAGE_VERSION) $(HELM_ARGS)

.PHONY: helm-set-bumped-version
helm-set-bumped-version:
	test -f charts/$(CHART_NAME)/Chart.yaml \
		&& ($(CPD_PATH) helm setver --chart charts/$(CHART_NAME)/Chart.yaml --version $(BUMPED_CHART_VERSION) &&\
			git add charts/$(CHART_NAME)/Chart.yaml) \
		|| true

.PHONY: helm-set-version
helm-set-version:
	test -f charts/$(CHART_NAME)/Chart.yaml \
		&& ($(CPD_PATH) helm setver --chart charts/$(CHART_NAME)/Chart.yaml --version $(CHART_VERSION) &&\
			git add charts/$(CHART_NAME)/Chart.yaml) \
		|| true

.PHONY: helm-release-local
## Set the version to the current un-bumped version and package
helm-release-local: helm-set-version helm-release

$(HOME)/.helm/repository/cache/helm-cloud-index.yaml:
	@echo helm repo helm-cloud repo missing, adding...
ifeq ($(HELM_USER)$(HELM_APIKEY),$(_empty))
ifeq ($(CI),true)
	@echo HELM_USER and HELM_APIKEY must be set on CI
	false
else
	$(eval user := $(shell bash -c 'read -p "Artifactory Email: " user; echo $$user'))
	$(eval pass := $(shell bash -c 'read -p "Artifactory API Key: " pass; echo $$pass'))
	@helm repo add helm-cloud https://confluent.jfrog.io/confluent/helm-cloud --username $(user) --password $(pass)
endif
else
	@helm repo add helm-cloud https://confluent.jfrog.io/confluent/helm-cloud --username $(HELM_USER) --password $(HELM_APIKEY)
endif

$(HOME)/.helm/repository/cache/incubator-index.yaml:
	@echo helm repo incubator repo missing, adding...
	@helm repo add incubator https://kubernetes-charts-incubator.storage.googleapis.com/

.PHONY: helm-update-repo
helm-update-repo: $(HOME)/.helm/repository/cache/helm-cloud-index.yaml $(HOME)/.helm/repository/cache/incubator-index.yaml
	@helm repo update

.PHONY: helm-install-deps
helm-install-deps: helm-update-repo
	helm dep build charts/$(CHART_NAME)

.PHONY: helm-update-deps
helm-update-deps: helm-update-repo
	rm -f charts/$(CHART_NAME)/requirements.lock && helm dep update charts/$(CHART_NAME)

.PHONY: helm-add-requirements-lock
helm-add-requirements: helm-update-deps
	git ls-files -m | grep -q requirements.lock \
		&& git add charts/$(CHART_NAME)/requirements.lock \
		|| true

.PHONY: helm-package
## Build helm package at the current version
helm-package: helm-set-version helm-install-deps
	mkdir -p charts/package
	rm -rf charts/package/$(CHART_NAME)-$(CHART_VERSION).tgz
	helm package charts/$(CHART_NAME) -d charts/package

.PHONY: helm-release
helm-release: helm-package
	$(CPD_PATH) helm upart --package charts/package/$(CHART_NAME)-$(CHART_VERSION).tgz --repo helm-cloud

.PHONY: helm-install-ci
helm-install-ci:
	test ! -f $(CI_BIN)/helm \
		&& wget -q $(HELM_URL)/$(HELM_TGZ) && tar xzfv $(HELM_TGZ) && mv linux-amd64/helm $(CI_BIN) \
		|| true

.PHONY: helm-init-ci
helm-init-ci:
	helm init --client-only

.PHONY: helm-setup-ci
helm-setup-ci: helm-install-ci helm-init-ci

.PHONY: helm-commit-deps
## Commit (and push) updated helm deps
helm-commit-deps:
	git diff --exit-code --name-status || \
		(git add charts/$(CHART_NAME)/requirements.lock && \
		git commit -m 'chore: $(UPSTREAM_CHART):$(UPSTREAM_VERSION) update chart deps' && \
		git push $(GIT_REMOTE_NAME) $(GIT_BRANCH_NAME))

.PHONY: $(HELM_DOWNSTREAM_CHARTS)
$(HELM_DOWNSTREAM_CHARTS):
ifeq ($(HOTFIX),true)
	@echo "Skipping bumping downstream helm chart deps $@ on hotfix branch"
else ifeq ($(BUMP),major)
	@echo "Skipping bumping downstream helm chart deps $@ with major version bump"
else
	git clone git@github.com:confluentinc/$@.git $@
	make -C $@ helm-update-deps helm-commit-deps \
		UPSTREAM_CHART=$(CHART_NAME) \
		UPSTREAM_VERSION=$(BUMPED_VERSION)
	rm -rf $@
endif
