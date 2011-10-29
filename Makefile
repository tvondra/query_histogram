MODULE_big = query_histogram
OBJS = query_histogram.o queryhist.o

EXTENSION = query_histogram
DATA = query_histogram--1.0.sql
MODULES = query_histogram

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: query_histogram.so

query_histogram.so: query_histogram.o queryhist.o

queryhist.o : queryhist.c

query_histogram.o: query_histogram.c
