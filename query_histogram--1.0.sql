CREATE OR REPLACE FUNCTION query_histogram( IN scale BOOLEAN DEFAULT TRUE, OUT bin_from INT, OUT bin_to INT, OUT bin_count INT, OUT bin_count_pct REAL,
                                            OUT bin_time REAL, OUT bin_time_pct REAL)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'query_histogram'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION query_histogram_reset()
    RETURNS void
    AS 'MODULE_PATHNAME', 'query_histogram_reset'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION query_histogram_get_reset()
    RETURNS timestamp
    AS 'MODULE_PATHNAME', 'query_histogram_get_reset'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE VIEW query_histogram AS SELECT * FROM query_histogram(true);