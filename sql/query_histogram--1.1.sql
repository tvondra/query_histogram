-- selects a histogram for a single database (databaseoid=NULL returns the global histogram)
CREATE OR REPLACE FUNCTION query_histogram (IN scale BOOLEAN DEFAULT TRUE, IN databaseoid OID DEFAULT NULL, OUT bin_from INT, OUT bin_to INT, OUT bin_count BIGINT, OUT bin_count_pct REAL,
                                            OUT bin_time DOUBLE PRECISION, OUT bin_time_pct REAL)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'query_histogram'
    LANGUAGE C IMMUTABLE;

-- selects histograms for all the databases (including global, which has databaseoid=NULL)
CREATE OR REPLACE FUNCTION query_histograms (IN scale BOOLEAN DEFAULT TRUE, OUT databaseoid OID, OUT bin_from INT, OUT bin_to INT, OUT bin_count BIGINT, OUT bin_count_pct REAL,
                                             OUT bin_time DOUBLE PRECISION, OUT bin_time_pct REAL)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'query_histograms'
    LANGUAGE C IMMUTABLE;

-- resets the whole histogram segment (all dbs, including global histogram)
CREATE OR REPLACE FUNCTION query_histogram_reset(remove BOOLEAN DEFAULT FALSE)
    RETURNS void
    AS 'MODULE_PATHNAME', 'query_histogram_reset'
    LANGUAGE C IMMUTABLE;

-- resets the global histogram
CREATE OR REPLACE FUNCTION query_histogram_reset_global()
    RETURNS void
    AS 'MODULE_PATHNAME', 'query_histogram_reset_global'
    LANGUAGE C IMMUTABLE;

-- resets histogram for a single database (optionally removes the histogram)
CREATE OR REPLACE FUNCTION query_histogram_reset_db(databaseoid OID, remove BOOLEAN DEFAULT FALSE)
    RETURNS void
    AS 'MODULE_PATHNAME', 'query_histogram_reset_db'
    LANGUAGE C IMMUTABLE;

-- returns version of the histogram (each change increments it)
CREATE OR REPLACE FUNCTION query_histogram_version()
    RETURNS bigint
    AS 'MODULE_PATHNAME', 'query_histogram_get_version'
    LANGUAGE C IMMUTABLE;

-- returns version of the histogram (each change increments it)
CREATE OR REPLACE FUNCTION query_histogram_get_reset_timestamp(IN databaseoid OID DEFAULT NULL)
    RETURNS timestamptz
    AS 'MODULE_PATHNAME', 'query_histogram_get_reset_timestamp'
    LANGUAGE C IMMUTABLE;

-- histogram for the current database
CREATE OR REPLACE VIEW query_histogram AS SELECT histogram.*, round(1000000*bin_time/(CASE WHEN bin_count > 0 THEN bin_count ELSE 1 END))/1000 AS bin_time_avg
    FROM query_histogram(true, (SELECT OID FROM pg_database WHERE datname = current_database())) histogram;

-- global histogram
CREATE OR REPLACE VIEW query_histogram_global AS SELECT histogram.*, round(1000000*bin_time/(CASE WHEN bin_count > 0 THEN bin_count ELSE 1 END))/1000 AS bin_time_avg
    FROM query_histogram(true) histogram;

-- histograms for all tracked databases (including global)
CREATE OR REPLACE VIEW query_histograms AS SELECT histogram.*, round(1000000*bin_time/(CASE WHEN bin_count > 0 THEN bin_count ELSE 1 END))/1000 AS bin_time_avg
    FROM query_histograms(true) histogram;