
SHELL := /bin/bash

SKARGO_PROFILE?=release

SCRIPT_DIR=$(shell dirname $(shell realpath $(firstword $(MAKEFILE_LIST))))
PRIVACY=$(realpath ../sql/privacy/init.sql)
SKDB=$(shell dirname $(SCRIPT_DIR))/sql/target/host/$(SKARGO_PROFILE)/skdb

SDKMAN_DIR?=$(HOME)/.sdkman
SDKMAN_INIT=$(SDKMAN_DIR)/bin/sdkman-init.sh
SERVER_PORT?=3586

DATABASES=$(SCRIPT_DIR)/ts/examples/databases/$(SERVER_PORT)

SERVER_DIR=$(realpath $(SCRIPT_DIR)/../sql/server/dev)
SERVER_JAR=$(SERVER_DIR)/build/libs/dev.jar
SERVER_CONFIG=$(DATABASES)/server.config

.PHONY: $(SERVER_CONFIG)
$(SERVER_CONFIG):
	mkdir -p $(DATABASES)
	@echo "skdb_port=$(SERVER_PORT)" > $(SERVER_CONFIG)
	@echo "skdb_databases=$(DATABASES)" >> $(SERVER_CONFIG)
	@echo "skdb=$(SKDB)" >> $(SERVER_CONFIG)
	@echo "skdb_init=$(PRIVACY)" >> $(SERVER_CONFIG)

.PHONY: build_server
build_server:
	@cd ../sql && skargo b --profile $(SKARGO_PROFILE)
	@cd $(SERVER_DIR) && source $(SDKMAN_INIT) && ../gradlew jar --no-daemon --console plain 1>&2 > $(DATABASES)/build_server.log

define skdb
$(SKDB) --data $(DATABASES)/$(1).db
endef

define exec
echo $(2); echo $(2) | $(call skdb,$(1))
endef

define run_server
source $(SDKMAN_INIT) && java -jar $(SERVER_JAR) --config $(SERVER_CONFIG) &> $(DATABASES)/server.log &
endef

define create_table
$(call exec,$(1),"CREATE TABLE IF NOT EXISTS \"$(2)\" (key JSON PRIMARY KEY,value JSON);")
endef

define create_input
$(call create_table,$(1),input$(2))
endef

clean: stop
	rm -rf $(DATABASES)

skdb-%: $(SKDB)
	$(call exec,$*,"$(QUERY)")

../build/sknpm:
	cd .. && make build/sknpm

.PHONY: skdb-ts-thin
skdb-ts-thin:
	cd ../skdb-ts-thin && npm i && tsc

.PHONY: check-src
check-src:
	cd ts/src && npm install
	cd ts/src && tsc
	cd ts/src && npm run lint

.PHONY: check-tests
check-tests:
	cd ts/tests && npm install
	cd ts/tests && tsc

.PHONY: check-all
check-all: check-src check check-tests

build: ../build/sknpm
	cd ts/examples && bun install
	../build/sknpm b -r --out-dir ts/examples/node_modules/skip-runtime

bunrun-%: ../build/sknpm
	cd ts/examples && bun install
	../build/sknpm b -r --out-dir ts/examples/node_modules/skip-runtime
	cd ts/examples && bun run $*.ts

bunclient-%:
	cd ts/examples && bun run $*-client.ts

bunrun-%-m: ../build/sknpm $(SERVER_CONFIG) start
	cd ts/examples && bun install
	../build/sknpm b -r --out-dir ts/examples/node_modules/skip-runtime
	cd ts/examples && bun run $*.ts $(SERVER_PORT)

check: ../build/sknpm
	../build/sknpm check
	../build/sknpm check --test

check-%: ../build/sknpm
	cd ts/examples && npm install
	../build/sknpm b -r --nowasm --out-dir ts/examples/node_modules/skip-runtime
	tsc --project ts/examples/tsconfig.json

noderun-%: ../build/sknpm
	cd ts/examples && npm install
	../build/sknpm b -r --out-dir ts/examples/node_modules/skip-runtime
	tsc --project ts/examples/tsconfig.json
	cd ts/examples && node dist/$*.js

nodeclient-%:
	cd ts/examples && node dist/$*-client.js

noderun-%-m: skdb-ts-thin ../build/sknpm  $(SERVER_CONFIG) start
	cd ts/examples && npm install
	../build/sknpm b -r --out-dir ts/examples/node_modules/skip-runtime
	tsc --project ts/examples/tsconfig.json
	cd ts/examples && node dist/$*.js $(SERVER_PORT)

.PHONY: start
start: build_server $(eval SPROCESSES:=$(shell bash -c 'source $(SDKMAN_INIT) && jps' | grep dev.jar | awk '{print $$1}'))
ifneq ($(SPROCESSES),)
	@kill $(SPROCESSES)
endif
	$(call run_server)


.PHONY: stop
stop: $(eval SPROCESSES:=$(shell bash -c 'source $(SDKMAN_INIT) && jps' | grep dev.jar | awk '{print $$1}'))
ifneq ($(SPROCESSES),)
	@kill $(SPROCESSES)
endif

stop-%: $(eval SPROCESSES:=$(shell bash -c 'source $(SDKMAN_INIT) && jps' | grep $*.jar | awk '{print $$1}'))
ifneq ($(SPROCESSES),)
	@kill $(SPROCESSES)
endif

.PHONY: jps
jps:
	source $(SDKMAN_INIT) && jps

.PHONY: used
used:
	sudo netstat -tulpn | grep LISTEN

