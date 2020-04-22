# Default version to install, new enough to self update
CPD_VERSION ?= v0.103.0
CPD_UPDATE ?= true

INIT_CI_TARGETS += cpd-update gcloud-install
TEST_TARGETS += test-cc-system
CLEAN_TARGETS += clean-cc-system-tests

# Set path for cpd binary
CPD_PATH := $(BIN_PATH)/cpd

ifeq ($(CI),true)
CPD_NAME ?= ci-$(subst /,_,$(SEMAPHORE_PROJECT_NAME))-$(BRANCH_NAME)-$(SEMAPHORE_BUILD_NUMBER)
CPD_EXPIRE ?= 2h
endif
CPD_NAME ?= random
CPD_EXPIRE ?= 24h

# Create Arguments
CPD_CR_ARGS ?= --deploy-umbrella=false --name $(CPD_NAME) --expire-time $(CPD_EXPIRE) --initial-size 15 --yes

# Deploy Arguments
ifeq ($(CHART_NAME),cc-umbrella-chart)
CPD_DEP_ARGS := --install-supporting=false --umbrella-chart-path='$(PWD)/charts/cc-umbrella-chart'
else
CPD_DEP_ARGS := --install-supporting=false --override-subchart-path='$(CHART_NAME)=$(PWD)/charts/$(CHART_NAME)'
endif

# Assume there is not an existing cluster when on CI
ifeq ($(CI),true)
CPD_RUNNING_COUNT := 0
else
CPD_RUNNING_COUNT := $(shell cpd priv ls --format json | jq '. | length')
endif

# Only run system tests if enabled
RUN_SYSTEM_TESTS ?= false

# Delete CPD environment after system tests
DELETE_CPD ?= true

# system test variables
CC_SYSTEM_TESTS_URI ?= git@github.com:confluentinc/cc-system-tests.git
CC_SYSTEM_TESTS_REF ?= $(shell (test -f CC_SYSTEM_TESTS_VERSION && head -n 1 CC_SYSTEM_TESTS_VERSION) || echo master)

.PHONY: show-cpd
## Show cpd vars
show-cpd:
	@echo "cpd version: $(CPD_VERSION)"
	@echo "cpd path: $(CPD_PATH)"
	@echo "cpd name: $(CPD_NAME)"
	@echo "cpd expire: $(CPD_EXPIRE)"
	@echo "cpd create args: $(CPD_CR_ARGS)"
	@echo "cpd deploy args: $(CPD_DEP_ARGS)"
	@echo "cpd running count: $(CPD_RUNNING_COUNT)"
	@echo "cc-system-tests run: $(RUN_SYSTEM_TESTS)"
	@echo "cc-system-tests uri: $(CC_SYSTEM_TESTS_URI)"
	@echo "cc-system-tests ref: $(CC_SYSTEM_TESTS_REF)"
	@echo "cc-system-tests delete: $(DELETE_CPD)"

.PHONY: gcloud-install
gcloud-install:
ifeq ($(CI),true)
	echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(shell lsb_release -c -s) main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
	curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
	install-package -n google-cloud-sdk kubectl
	yes n | gcloud init --console-only --skip-diagnostics
	gcloud config set project cloud-private-dev
	gcloud config set account semaphore@cloud-private-dev.iam.gserviceaccount.com
	gcloud auth activate-service-account --key-file ~/.config/gcloud/application_default_credentials.json
endif

.PHONY: cpd-install
# Install cpd if it's not installed
cpd-install:
	@test -f $(CPD_PATH) ||\
		(aws --profile default s3 cp s3://cloud-confluent-bin/cpd/cpd-$(CPD_VERSION)-$(shell go env GOOS)-$(shell go env GOARCH) $(CPD_PATH) && \
		chmod +x $(CPD_PATH))

.PHONY: cpd-update
# Update cpd if needed, install if missing
cpd-update: cpd-install
ifeq ($(CPD_UPDATE),true)
	$(CPD_PATH) update --yes
endif

.PHONY: cpd-priv-create
# Create a private environment for testing
cpd-priv-create:
	$(CPD_PATH) priv cr $(CPD_CR_ARGS)

.PHONY: cpd-priv-create-if-missing
ifeq ($(CPD_RUNNING_COUNT),0)
# Create a private environment for testing only if there are none
cpd-priv-create-if-missing: cpd-priv-create
else
cpd-priv-create-if-missing:
endif

.PHONY: cpd-deploy-local
## Deploy local chart to cpd cluster
cpd-deploy-local: cpd-update helm-update-repo cpd-priv-create-if-missing
ifeq ($(CI),true)
	# On CI the name should be unique, get it by that
	$(eval CPD_RUNNING_ID := $(shell $(CPD_PATH) priv ls --name $(CPD_NAME) --format id-list --limit 1 ))
else
	# If we're not on CI, name is random, just pick the first one we find unless this var is already set
	$(eval CPD_RUNNING_ID ?= $(shell $(CPD_PATH) priv ls --format id-list --limit 1))
endif
	$(CPD_PATH) priv dep --id $(CPD_RUNNING_ID) $(CPD_DEP_ARGS)

.PHONY: cpd-destroy
## Clean up all cpd clusters
cpd-destroy:
	$(CPD_PATH) priv ls --format json | jq -r '.[].Id' | xargs -IID $(CPD_PATH) priv del --id ID --yes

.cc-system-tests:
	git clone $(CC_SYSTEM_TESTS_URI) .cc-system-tests

.PHONY: checkout-cc-system-tests
checkout-cc-system-tests: .cc-system-tests
	git -C ./.cc-system-tests fetch origin
	git -C ./.cc-system-tests checkout $(CC_SYSTEM_TESTS_REF)
	git -C ./.cc-system-tests merge origin/$(CC_SYSTEM_TESTS_REF)
	@echo "cc-system-tests last commit:"
	@git -C ./.cc-system-tests log -n 1

define _newline


endef

.PHONY: test-cc-system
ifeq ($(RUN_SYSTEM_TESTS),true)
## Run cc-system tests
test-cc-system: checkout-cc-system-tests helm-set-version cpd-deploy-local
	$(eval $(subst ||,$(_newline),$(shell $(CPD_PATH) priv testenv --id $(CPD_RUNNING_ID) --ready-wait-timeout 600 --output-format make --separator '||')))
	@env | grep -E 'CREATE_|CCLOUD_|METRICS_'
	make -C ./.cc-system-tests init-env test
ifeq ($(DELETE_CPD),true)
	make cpd-destroy
endif
else
test-cc-system:
	true
endif

.PHONY: clean-cc-system-tests
## Clean up .cc-system-tests folder
clean-cc-system-tests:
	rm -rf .cc-system-tests
