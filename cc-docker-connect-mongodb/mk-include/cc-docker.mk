_empty :=
_space := $(_empty) $(empty)

# Base Stuff
BASE_IMAGE ?=
BASE_VERSION ?=

# Image Name
IMAGE_NAME ?= unknown
ifeq ($(IMAGE_NAME),unknown)
$(error IMAGE_NAME must be set)
endif

# Image Version
#  If we're on CI and a release branch, build with the bumped version
ifeq ($(CI),true)
ifneq ($(RELEASE_BRANCH),$(_empty))
IMAGE_VERSION ?= $(BUMPED_VERSION)
else
IMAGE_VERSION ?= $(VERSION)
endif
else
IMAGE_VERSION ?= $(VERSION)
endif

IMAGE_REPO ?= confluentinc
ifeq ($(IMAGE_REPO),$(_empty))
BUILD_TAG ?= $(IMAGE_NAME):$(IMAGE_VERSION)
BUILD_TAG_LATEST ?= $(IMAGE_NAME):latest
else
BUILD_TAG ?= $(IMAGE_REPO)/$(IMAGE_NAME):$(IMAGE_VERSION)
BUILD_TAG_LATEST ?= $(IMAGE_REPO)/$(IMAGE_NAME):latest
endif

DOCKER_REPO ?= confluent-docker.jfrog.io

# Set targets for standard commands
RELEASE_POSTCOMMIT += push-docker
BUILD_TARGETS += build-docker
CLEAN_TARGETS += clean-images

DOCKER_BUILD_PRE ?=
DOCKER_BUILD_POST ?=

.PHONY: show-docker
## Show docker variables
show-docker:
	@echo "BASE_IMAGE: $(BASE_IMAGE)"
	@echo "BASE_VERSION: $(BASE_VERSION)"
	@echo "IMAGE_NAME: $(IMAGE_NAME)"
	@echo "IMAGE_VERSION: $(IMAGE_VERSION)"
	@echo "IMAGE_REPO: $(IMAGE_REPO)"
	@echo "BUILD_TAG: $(BUILD_TAG)"
	@echo "BUILD_TAG_LATEST: $(BUILD_TAG_LATEST)"
	@echo "DOCKER_REPO: $(DOCKER_REPO)"

.PHONY: docker-login
## Login to docker Artifactory
docker-login:
ifeq ($(DOCKER_USER)$(DOCKER_APIKEY),$(_empty))
	@jq -e '.auths."confluent-docker.jfrog.io"' $(HOME)/.docker/config.json 2>&1 >/dev/null ||\
		(echo "confluent-docker.jfrog.io not logged in, Username and Password not found in environment, prompting for login:" && \
		 docker login confluent-docker.jfrog.io)
else
	@jq -e '.auths."confluent-docker.jfrog.io"' $(HOME)/.docker/config.json 2>&1 >/dev/null ||\
		docker login confluent-docker.jfrog.io --username $(DOCKER_USER) --password $(DOCKER_APIKEY)
endif

.PHONY: docker-pull-base
## Pull the base docker image
docker-pull-base: docker-login
ifneq ($(BASE_IMAGE),$(_empty))
	docker image ls -f reference="$(BASE_IMAGE):$(BASE_VERSION)" | grep -Eq "$(BASE_IMAGE)[ ]*$(BASE_VERSION)" || \
		docker pull $(BASE_IMAGE):$(BASE_VERSION)
endif

.PHONY: build-docker
ifeq ($(BUILD_DOCKER_OVERRIDE),)
## Build just the docker image
build-docker: .netrc .ssh docker-pull-base $(DOCKER_BUILD_PRE)
	docker build --no-cache \
		--build-arg version=$(IMAGE_VERSION) \
		--build-arg base_version=$(BASE_VERSION) \
		--build-arg base_image=$(BASE_IMAGE) \
		-t $(BUILD_TAG) .
	rm -rf .netrc .ssh
ifneq ($(DOCKER_BUILD_POST),)
	make $(DOCKER_BUILD_POST)
endif
else
build-docker: $(BUILD_DOCKER_OVERRIDE)
endif

.PHONY: tag-docker
tag-docker: tag-docker-latest tag-docker-version

.PHONY: tag-docker-latest
tag-docker-latest:
	@echo 'create docker tag latest'
	docker tag $(BUILD_TAG) $(DOCKER_REPO)/$(BUILD_TAG_LATEST)

.PHONY: tag-docker-version
tag-docker-version:
	@echo 'create docker tag $(IMAGE_VERSION)'
	docker tag $(BUILD_TAG) $(DOCKER_REPO)/$(BUILD_TAG)

.PHONY: push-docker
ifeq ($(PUSH_DOCKER_OVERRIDE),)
push-docker: push-docker-latest push-docker-version
else
push-docker: $(PUSH_DOCKER_OVERRIDE)
endif

.PHONY: push-docker-latest
push-docker-latest: tag-docker-latest
	@echo 'push latest to $(DOCKER_REPO)'
	docker push $(DOCKER_REPO)/$(BUILD_TAG_LATEST)

.PHONY: push-docker-version
## Push the current version of docker to artifactory
push-docker-version: tag-docker-version
	@echo 'push $(IMAGE_VERSION) to $(DOCKER_REPO)'
	docker push $(DOCKER_REPO)/$(BUILD_TAG)

.PHONY: clean-images
clean-images:
	docker images -q -f label=io.confluent.caas=true -f reference='*$(IMAGE_NAME)' | uniq | xargs docker rmi -f

.PHONY: clean-all
clean-all:
	docker images -q -f label=io.confluent.caas=true | uniq | xargs docker rmi -f
