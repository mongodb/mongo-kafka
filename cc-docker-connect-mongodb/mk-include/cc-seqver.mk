_empty := 
_space := $(_empty) $(empty)

VERSION := $(shell [ -d .git ] && git describe --tags --always --dirty --match v\*)
CLEAN_VERSION := $(shell echo $(VERSION) | grep -Eo 'v[0-9]+' | grep -Eo '[0-9]+')
CI_SKIP ?= [ci skip]

ifeq ($(CLEAN_VERSION),$(_empty))
CLEAN_VERSION := 0
endif

BUMPED_VERSION := v$(shell expr $(CLEAN_VERSION) + 1)

RELEASE_SVG := <svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="94" height="20"><linearGradient id="b" x2="0" y2="100%"><stop offset="0" stop-color="\#bbb" stop-opacity=".1"/><stop offset="1" stop-opacity=".1"/></linearGradient><clipPath id="a"><rect width="94" height="20" rx="3" fill="\#fff"/></clipPath><g clip-path="url(\#a)"><path fill="\#555" d="M0 0h49v20H0z"/><path fill="\#007ec6" d="M49 0h45v20H49z"/><path fill="url(\#b)" d="M0 0h94v20H0z"/></g><g fill="\#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="110"><text x="255" y="150" fill="\#010101" fill-opacity=".3" transform="scale(.1)" textLength="390">release</text><text x="255" y="140" transform="scale(.1)" textLength="390">release</text><text x="705" y="150" fill="\#010101" fill-opacity=".3" transform="scale(.1)" textLength="350">$(BUMPED_VERSION)</text><text x="705" y="140" transform="scale(.1)" textLength="350">$(BUMPED_VERSION)</text></g> </svg>

version:
	@echo $(VERSION)

show-version:
	@echo version: $(VERSION)
	@echo clean version: $(CLEAN_VERSION)
	@echo bumped version: $(BUMPED_VERSION)
	@echo 'release svg: $(RELEASE_SVG)'

tag-release:
	git tag $(BUMPED_VERSION)
	git push $(GIT_REMOTE_NAME) $(RELEASE_BRANCH) --tags

get-release-image:
	echo '$(RELEASE_SVG)' > release.svg
	git add release.svg

commit-release:
	git diff --exit-code --cached --name-status || \
	git commit -m "$(BUMPED_VERSION): $(BUMP) version bump $(CI_SKIP)"
