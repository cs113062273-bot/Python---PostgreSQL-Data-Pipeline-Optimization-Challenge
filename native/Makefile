MODULE_big = challenge_probe
OBJS = challenge_probe.o
PG_CONFIG ?= pg_config
PG_CPPFLAGS += -DCHALLENGE_PROBE_SOURCE_SHA='"$(shell sha256sum challenge_probe.c | cut -d" " -f1)"'
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
