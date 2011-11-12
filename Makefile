MODULE_big = query_histogram
OBJS = src/query_histogram.o src/queryhist.o

EXTENSION = query_histogram
DATA = sql/query_histogram--1.1.sql
MODULES = query_histogram

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: query_histogram.so

query_histogram.so: $(OBJS)

%.o : src/%.c
