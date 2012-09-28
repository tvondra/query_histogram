Query Histogram Extension
=========================
This is a PostgreSQL extension providing a simple histogram of queries
(according to their duration). Once you install the connection, you
may use three functions and four GUC to control the histogram - get
data, reset it, change the bin size, sampling etc.

The histogram data are stored in a shared memory segment (so that all
backends may share it and it's not lost in case of on disconnections).
The segment is quite small (about 8kB of data) and it's protected by
a System V semaphore. That might cause some performance problems - to
minimize this issue, you may sample only some of the queries (see the
`sample_pct` GUC variable).

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

* `query_histogram()`        - get data
* `query_histogram_reset()`  - reset data, start collecting again

The first one is the most important one, as it allows you to read the
current histogram data - just use it as a table:

    db=# SELECT * FROM query_histogram();

The columns of the result are rather obvious:

* `bin_from`, `bin_to` - bin range (from, to) in miliseconds

* `bin_count` - number of queries in the bin

* `bin_count_pct` - number of queries proportionaly to the total number
                     in the histogram
* `bin_time` - time accumulated by queries in the bin

* `bin_time_pct` - time accumulated by queries in the bin proportionaly
                    to the total time (accumulted by all queries)

The second function may be handy if you need to reset the histogram and
start collecting again (for example you may collect the stats regularly
and reset it).
