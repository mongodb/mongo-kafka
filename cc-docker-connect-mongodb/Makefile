MVN_PROFILE ?= confluent-nexus
IMAGE_NAME := cc-connect-mongodb
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/cc-built-base
BASE_VERSION := 0.0.2
BUILD_DOCKER_OVERRIDE := build-mvn-docker
PUSH_DOCKER_OVERRIDE := push-connect-docker
INIT_CI_TARGETS := install-mvn

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-end.mk

.PHONY: cc-connect-mongodb
cc-connect-mongodb:
	[ -d kafka-connect-mongodb ] || git clone git@github.com:mongodb/mongo-kafka.git kafka-connect-mongodb
	git -C kafka-connect-mongodb checkout master
	git -C kafka-connect-mongodb pull origin master
	cd kafka-connect-mongodb && ./gradlew clean createConfluentArchive
	unzip -d ./kafka-connect-mongodb/build/confluent ./kafka-connect-mongodb/build/confluent/*.zip
	find ./kafka-connect-mongodb/build/confluent -maxdepth 1 -type d ! -wholename "./kafka-connect-mongodb/build/confluent" -exec mv {} ./kafka-connect-mongodb/build/confluent/kafka-connect-mongodb \;
	docker build \
		-t confluentinc/cc-connect-mongodb:$(IMAGE_VERSION) \
		--build-arg BASE_VERSION=$(BASE_VERSION) \
		--build-arg DOCKER_REGISTRY=$(DOCKER_REPO)/ \
		--label MG_VERSION=$$(git -C kafka-connect-mongodb rev-parse --short HEAD) .

.PHONY: build-mvn-docker
build-mvn-docker: docker-pull-base cc-connect-mongodb

.PHONY: push-connect-docker
push-connect-docker: push-docker-latest push-docker-version

.PHONY: install-mvn
install-mvn:
	sudo apt-get update && sudo apt-get -y install maven openjdk-8-jdk
