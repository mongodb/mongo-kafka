PROTOC := $(BIN_PATH)/protoc
PROTOC_VERSION := 3.5.0
PROTOC_INSTALLED_VERSION := $(shell $(PROTOC) --version 2>/dev/null protoc | awk '{print $$2}')

uname := $(shell uname)
ifeq ($(uname),Darwin)
PROTOC_OS := osx
else ifeq ($(uname),Linux)
PROTOC_OS := linux
endif

PROTOC_ARCH := $(shell uname -m)

.PHONY: install-protoc
install-protoc:
ifneq ($(PROTOC_VERSION),$(PROTOC_INSTALLED_VERSION))
	curl -L -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v3.5.0/protoc-$(PROTOC_VERSION)-$(PROTOC_OS)-$(PROTOC_ARCH).zip
	(cd /tmp && unzip protoc.zip bin/protoc)
	mv /tmp/bin/protoc $(PROTOC)
endif
