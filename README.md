Query Histogram Extension
=========================
This is a PostgreSQL extension providing a simple histogram of queries
(according to their duration). Once you install the extension, you
may use several functions and four GUC to control the histograms - get
data, reset it, change the bin size, sampling etc.

The histogram data are stored in a shared memory segment (so that all
backends may share it). The segment size depends on the maximum number
of tracked databases / number of bins etc. In general it should not be
larger than a few MBs for hundreds of databases with ~1000 bins.

The segment and per-db histograms are protected by LW Locks, but this
should not cause any performance issues. The worst impact I've measured
so far is ~2.5% on a "pgbench -S" benchmark with many extremely short
queries (~1ms). On a more realistic workloads (with longer queries)
the overhead is practically non-existent.

However if you run into a performance issue because of collecting the
histogram data, you may sample only some of the queries (see the
`sample_pct` GUC variable). For example sampling only 5% should help
a lot while still providing good overview of the workload.

Most of the code that interacts directly with the executor comes from
the `auto_explain` and `pg_stat_statements` extensions (hooks, shared
memory management etc).


Install
-------
Installing the extension is quite simple, especially if you're using
pgxn client (which I recommend) - all you need to do is

    $ pgxn install query_histogram
    $ pgxn load -d mydb query_histogram

but you may also do the installation manually like this:

    $ make install

and the (after connecting to the database)

    db=# CREATE EXTENSION query_histogram;

If you're on pre-9.1 version, you'll have to do the second part manually
by running the SQL script (`query_histogram--x.y.sql`) in the database. If
needed, replace `MODULE_PATHNAME` by $libdir.


Config
------
Now the functions are created, but you still need to load the shared
module. This needs to be done from postgresql.conf, as the module
needs to allocate space in the shared memory segment. So add this to
the config file (or update the current values)

    # libraries to load
    shared_preload_libraries = 'query_histogram'

    # known GUC prefixes (not needed since 9.2)
    custom_variable_classes = 'query_histogram'

    # config of the query histogram
    query_histogram.sample_pct = 5
    query_histogram.bin_width = 10
    query_histogram.bin_count = 1000
    query_histogram.dynamic = true

And then restart the database to actually load the shared module.

The meaning of those config options is this:

* `query_histogram.sample_pct` - sampling rate, i.e. how many
  queries will be actually inserted into the histogram (this
  involves locking, so you may use lower values to limit the
  impact if this is a problem)

* `query_histogram.bin_count` - number of bins (0-1000), 0 means
  the histogram is disabled (still, the hooks are installed
  so there is some overhead - if you don't need the histogram
  remove it from shared_preload_libraries)

* `query_histogram.bin_width` - width of each bin (in miliseconds)

* `query_histogram.dynamic` - if you set this to false, then you
  won't be able to dynamically change the histogram options
  (number of bins, sampling rate etc.) set in the config file

When you set the histogram to dynamic=true, you may change the
histogram on the fly, but it adds overhead because whenever you need
to access the `sample_pct`, it has to be loaded from the shared segment.
So the segment has to be locked etc.

Again, this is an option that allows you to reduce the overhead. If
you're afraid the overhead might be an issue, use dynamic=false and
low `sample_pct` (e.g. 5).

If you prefer flexibility and exact overview of the queries, use high
`sample_pct` (even 100) and dynamic=true (so that you may reconfigure
the queries using SET commands).

So if you want a histogram with 100 bins, each bin 1 second wide, and
you've set 'dynamic=true', you may do this

    db=# SET query_histogram.bin_count = 100;
    db=# SET query_histogram.bin_width = 1000;

And if you want to sample just 1% of the queries, you may do this

    db=# SET query_histogram.sample_pct = 1;

You can't change the 'dynamic' option (except directly in the file).


Reading the histogram data
--------------------------
There are two functions that you can use to work with the histogram.

* `query_histogram()`   - get histogram for a single database (or global)
* `query_histograms()`  - get histograms for all tracked databases

Or you may use one of the predefined views:

* `query_histogram`         - histogram for the current db
* `query_histogram_global`  - global histogram (aggregating all dbs)
* `query_histograms`        - histograms for all databases

The main columns returned by these functions and views are these:

* `databaseoid` - OID of the database (NULL for global histogram),
                  returned only from `query_histograms`

* `bin_from`, `bin_to` - bin range (from, to) in miliseconds

* `bin_count` - number of queries in the bin

* `bin_count_pct` - number of queries proportionaly to the total number
                    in the histogram
* `bin_time` - time accumulated by queries in the bin

* `bin_time_pct` - time accumulated by queries in the bin proportionaly
                   to the total time (accumulted by all queries)

* `bin_time_avg` - average time of queries in this bin (rounded to ms)

All the functions have `scale` parameter, which is important when using
`sample_pct` with values other than 100 (i.e. when sampling only portion
of the queries). With `scale=true` the data are scaled as if all queries
were sampled. With `scale=true` only the actually sampled data (counts
and durations) are returned. All the views use `scale=true`.


Resetting histogram(s)
----------------------
From time to time you may need to throw away all the collected data and
start over. In that case you may use these functions to reset either
one or all the histograms:

* `query_histogram_reset(oid, bool)` - reset all histograms (optionally
                                       reset list of tracked dbs)

* `query_histogram_reset_global(oid, boolean)` - reset global histogram

* `query_histogram_reset_db(oid, bool)` - reset global histogram

The second function may be handy if you need to reset the histogram and
start collecting again (for example you may collect the stats regularly
and reset it).


Persisting histogram data
-------------------------
On each server shutdown, histogram data are persisted into a file (in
the $DATA/global directory). And of course, on each server startup,
the extension attempts to read the data from the file. That means that
the histogram is preserved across server restarts (at least for
clean shutdowns).

Reading the file is meant to be "permissive" - that is, accepting even
histograms with different parameters (than the ones in server). For
example it's possible to lower maximum number of databases, as long as
the number of databases actually tracked in the file is lower.

So if you find out you've initially set query_histogram.max_databases
too high (and you're only using small part of the reserved memory),
then you can lower the GUC value and restart the server.

Of course this is not perfect and there are combinations that are not
acceptable (at least for now).


Known issues
------------
The extension is (currently) unable to react to a DROP DATABASE. If the
database is tracked (there's a histogram for it), this histogram will
remain there until removed explicitly (e.g. by calling one of the reset
functions). The easiest way to do that is a query like this

    SELECT query_histogram_reset(oid, true) FROM (
        SELECT databaseoid
          FROM query_histograms LEFT JOIN pg_database
                                       ON (oid = databaseoid)
         WHERE oid IS NULL
    ) dropped_databases;

which selects databases with tracked histograms but missing in the
system catalog, and then resets the histogram. If you're dropping
a lot of databases, it might be a good idea putting this into cron.

Maybe in the future it'll be possible to use an event trigger, but until
then this is the best solution I'm aware of.
