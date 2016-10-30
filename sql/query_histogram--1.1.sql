CREATE OR REPLACE FUNCTION query_histogram( IN scale BOOLEAN DEFAULT TRUE, OUT bin_from INT, OUT bin_to INT, OUT bin_count BIGINT, OUT bin_count_pct REAL,
                                            OUT bin_time DOUBLE PRECISION, OUT bin_time_pct REAL)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'query_histogram'
    LANGUAGE C IMMUTABLE;
    
CREATE OR REPLACE FUNCTION xact_histogram( IN scale BOOLEAN DEFAULT TRUE, OUT bin_from INT, OUT bin_to INT, OUT bin_count BIGINT, OUT bin_count_pct REAL,
                                            OUT bin_time DOUBLE PRECISION, OUT bin_time_pct REAL)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'xact_histogram'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION query_histogram_reset()
    RETURNS void
    AS 'MODULE_PATHNAME', 'query_histogram_reset'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION query_histogram_get_reset()
    RETURNS timestamp
    AS 'MODULE_PATHNAME', 'query_histogram_get_reset'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE VIEW query_histogram AS
    SELECT
        histogram.*,
        round(1000000 * bin_time / (CASE WHEN bin_count > 0 THEN bin_count ELSE 1 END)) / 1000 AS bin_time_avg
    FROM query_histogram(true) histogram;

CREATE OR REPLACE VIEW xact_histogram AS
    SELECT
        histogram.*,
        round(1000000 * bin_time / (CASE WHEN bin_count > 0 THEN bin_count ELSE 1 END)) / 1000 AS bin_time_avg
    FROM xact_histogram(true) histogram;
