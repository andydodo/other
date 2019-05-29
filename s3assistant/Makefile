GOCMD := go
GOBUILD := $(GOCMD) build
GORUN := $(GOCMD) run
GOCLEAN := $(GOCMD) clean
GOGET := $(GOCMD) get
GOFLAGS := -v -ldflags
BINARY := s3assistant
OUTPATH := output
PACKAGE := s3assistant
GITCOMMIT := $(shell git rev-list HEAD | head -n1)
COMPILEDATE := $(shell date '+%Y-%m-%d %H:%M:%S')
APPVERSION	:= $(shell git tag | xargs -I@ git log --format=format:'%ai @%n' -1 @ | sort | awk '{print $$4}'| tail -n1)

.PHONY: all build clean 
all: build

build: 
	mkdir -p $(OUTPATH)
	$(GOBUILD) -o $(OUTPATH)/$(BINARY) $(GOFLAGS) \
		"-X 'main.GIT_COMMIT=${GITCOMMIT}' \
		-X 'main.BUILD_TIME=${COMPILEDATE}' \
		-X 'main.APP_VERSION=${APPVERSION}'"

clean: 
	$(GOCLEAN)
	rm -rf $(OUTPATH)
