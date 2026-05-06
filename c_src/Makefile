# Builds priv/erlfdb_worker - the standalone executable that BEAM launches
# as a port subprocess. Invoked from rebar.config via pre_hooks.
#
# Env vars expected from the caller (rebar.config.script):
#   ERLFDB_INCLUDE_DIR     - FDB C headers (default /usr/local/include)
#   ERLFDB_FDB_LIB_DIR     - directory containing libfdb_c.{so,dylib}
#   ERLFDB_FDB_API_VERSION - integer like 730 (omit for FDB_USE_LATEST_API_VERSION)
#
# Erlang/ei paths discovered at build time via `erl`.
#
# Quoting note: every path-bearing variable is wrapped in double quotes when
# it appears in a recipe's shell command, so paths containing spaces or
# other shell metacharacters work. Make itself doesn't tolerate spaces in
# target/dep names, but our internal targets are space-free relative paths
# (../priv/erlfdb_worker, *.worker.o); only the externally-discovered
# include/lib directories are at risk and they're only used as compiler/
# linker flag values.

PRIV_DIR := ../priv
TARGET   := $(PRIV_DIR)/erlfdb_worker

# Discover Erlang installation paths
ERTS_INCLUDE := $(shell erl -noshell -eval 'io:format("~ts", [filename:join([code:root_dir(), "erts-" ++ erlang:system_info(version), "include"])])' -s init stop)
EI_INCLUDE   := $(shell erl -noshell -eval 'io:format("~ts", [code:lib_dir(erl_interface, include)])' -s init stop)
EI_LIB       := $(shell erl -noshell -eval 'io:format("~ts", [code:lib_dir(erl_interface, lib)])' -s init stop)

# Detect platform for rpath flavor
UNAME_S := $(shell uname -s)

ERLFDB_INCLUDE_DIR ?= /usr/local/include
ERLFDB_FDB_LIB_DIR ?= /usr/local/lib

# Compiler/linker flags. Each `-I.../-L...` is constructed with the path
# wrapped in double quotes inside the recipe - see below.
COMMON_DEFINES :=
ifneq ($(ERLFDB_FDB_API_VERSION),)
COMMON_DEFINES += -DFDB_API_VERSION=$(ERLFDB_FDB_API_VERSION)
else
COMMON_DEFINES += -DFDB_USE_LATEST_API_VERSION=1
endif

CFLAGS_COMMON := -Wall -Werror -g -O2 -std=c11 $(COMMON_DEFINES)

LDFLAGS_LIBS := -lei -lpthread -L"$(ERLFDB_FDB_LIB_DIR)" -lfdb_c

ifeq ($(UNAME_S),Darwin)
RPATH_SELF      := @loader_path
RPATH_FDB_FLAGS := -Wl,-rpath,@loader_path -Wl,-rpath,"$(ERLFDB_FDB_LIB_DIR)"
else
RPATH_FDB_FLAGS := -Wl,-rpath,\$$ORIGIN -Wl,-rpath,"$(ERLFDB_FDB_LIB_DIR)"
endif

# Sources
WORKER_SRC := worker.c
WORKER_OBJ := $(WORKER_SRC:.c=.worker.o)

.PHONY: all clean

all: $(TARGET)

$(PRIV_DIR):
	mkdir -p "$(PRIV_DIR)"

%.worker.o: %.c
	$(CC) $(CFLAGS_COMMON) -I"$(ERTS_INCLUDE)" -I"$(EI_INCLUDE)" -I"$(ERLFDB_INCLUDE_DIR)" -c $< -o $@

$(TARGET): $(WORKER_OBJ) | $(PRIV_DIR)
	$(CC) $(WORKER_OBJ) -L"$(EI_LIB)" $(LDFLAGS_LIBS) $(RPATH_FDB_FLAGS) -o "$@"
	chmod +x "$@"

clean:
	rm -f $(WORKER_OBJ) "$(TARGET)"
