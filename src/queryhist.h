/* The following structures define structure of data stored in the shared
 * memory - parameters shared by the histograms, histogram data etc.
 *
 * The overall structure is rather simple, and consists of three stuctures
 *
 * (1) segment_info_t
 *      - global structure, with pointers to db info and histograms
 *      - array of db_info_t (info about databases)
 *      - array of histogram_info_t (histogram data)
 *
 * (2) db_info_t - info about tracked databases
 *
 * (3) histogram_info_t - data of a histogram (counts, durations)
 *
 * The arrays are placed right after the segment_info_t structure, and
 * the pointers are set accordingly.
 *
 *
 * When processing a query, this is what happens:
 *
 * (1)  lock segment_info_t in LW_SHARED mode
 *
 * (2)  get sample rate, decide whether to log this query (no => terminate)
 *
 * (3)  lock the global histogram (first element in array of histograms)
 *      in exclusive mode
 *
 * (4)  add query to the global histogram and unlock it
 *
 * (5)  check whether the database already is in the list of databases
 *
 * (6)  if it's a new database, there's space for another one, relock the
 *      segment in LW_EXCLUSIVE mode, add the new database, lock the
 *      histogram in LW_EXCLUSIVE mode and then relock the segment in
 *      LW_SHARED mode (still holding exclusive lock on histogram)
 *
 * (7)  lock the databse histogram in exclusive mode (unless already locked
 *      in step 6) and add the query to it
 *
 * (8)  unlock the histogram and then the segment
 *
 *
 * Similarly when reading the histogram(s), resetting the histograms etc.
 *
 * FIXME Spinlocks would be a better choice for the histogram locks - these
 *       locks should be held for only very short periods, usually we need
 *       exclusive access (shared suffices only when reading the histogram,
 *       which is a small fraction) and there's no contention between
 *       databases. For the segment itself the LWLock is most probably the
 *       right choice to allow shared access from multiple databases.
 *
 * FIXME Currently there's not a good way to respond to DROP DATABASE - if
 *       the database is tracked, the record will remain there until the
 *       whole histogram segment is reset. There should be a better way to
 *       do that - e.g. a function that removes a single database histogram
 *       using OID of the database.
 * 
 * FIXME I'm rather unhappy about the current handling of histogram bin
 *       counts (HIST_BINS_MAX vs. bin_count etc.). IMHO the current code
 *       is rather messy and needs to be cleared / fixed.
 */

#include "postgres.h"
#include "tcop/utility.h"
#include "utils/timestamp.h"
#include "storage/lwlock.h"

/* TODO When the histogram is static (dynamic=0), we may actually
 *      use less memory because the use can't resize it (so the
 *      bin_count is actually the only possible size). */

/* 1000 bins ought be enough for anyone ;-) */
#define HIST_BINS_MAX 1000
#define HISTOGRAM_DUMP_FILE "global/query_histogram.stat"

/* How are the histogram bins scaled? */
typedef enum {
    HISTOGRAM_LINEAR,
    HISTOGRAM_LOG
} histogram_type_t;

/* data types used to store queries */
typedef int64   count_bin_t;
typedef float8  time_bin_t;

/* info about one histogram (either global or per-database)
 *
 * is part of the segment_info_t, initialized at shmem statup */
typedef struct histogram_info_t {

    /* time of the last reset of this particular histogram */
    TimestampTz  last_reset;

    /* lock guarding the histogram structure (need to be here, because global
     * histogram has no db_info_t record)
     *
     * TODO try replacing these histogram-level locks with spinslocks */
    LWLockId    lock;

    /* histogram data - counts and durations per bin
     *
     * TODO a structure with count and duration fields might be better */
    count_bin_t count_bins[HIST_BINS_MAX+1];
    time_bin_t  time_bins[HIST_BINS_MAX+1];

} histogram_info_t;

/* info about a tracked database - OID, lock, pointer to a histogram
 *
 * is part of the segment_info_t, initialized at shmem statup */
typedef struct db_info_t {

    /* OID of the database */
    Oid databaseoid;

    /* index of the histogram for this database */
    int histogram_idx;

} db_info_t;

/* info about the whole shared segment, that is
 *
 * - structure of the shared memory segment (list of databases, histograms)
 * - global limits (number of bins, max databases, ...)
 * - shared histogram parameters */
typedef struct segment_info_t {

    /* lock guarding the segment (shared info and all the histograms) */
    LWLockId    lock;

    /* max number of databases we can track (we have enough reserved memory) */
    int max_databases; /* 0 => track only global histogram */

    /* already allocated databases (up to max_databases) */
    int current_databases;

    /* every time the segment-level info is modified (complete reset, added or removed
     * a database, ...), this is incremented so that all backends know to lookup the
     * database index again (int64 is probably an overkill, but meh ...) */
    int64   version;

    /* all the histograms need to use exactly the same parameters (for now)
     *
     * TODO: maybe this could be improved in the future (seems difficult, though) */
    int  type;          /* current type (LINEAR or LOGARITMIC) */
    int  bins;          /* current number of bins */
    int  step;          /* bin width */
    int  sample_pct;    /* sampling rate */
    bool track_utility; /* track utility commands? */

    /* pointer to array info about tracked databases (fixed size: current_databases items) */
    db_info_t * databases;

    /* pointer to array of histograms - global + one for each database (fixed size: max_databases + 1)*/
    histogram_info_t * histograms;

} segment_info_t;

/* used to transfer the data to the SRF functions */
typedef struct histogram_data {

    /* LINEAR or LOGARITMIC */
    int histogram_type;

    /* OID of the database (unused for global histogram) */
    Oid databaseoid;

    /* number of bins, width of a bin */
    int32 bins_count;
    int32 bins_width;

    /* query counts (total, per-bin) */
    count_bin_t     total_count;
    count_bin_t    *count_data;

    /* query durations (total, per-bin) */
    time_bin_t      total_time;
    time_bin_t     *time_data;

    /* link to the next histogram */
    struct histogram_data * next;

} histogram_data;

/* how much memory we need for the histograms */
size_t histogram_segment_size(int max_databases);

/* number of LWLocks (segment + global + per DB) */
size_t histogram_segment_locks(int max_databases);

/* returns data for SRF - global histogram or per-db histograms */
histogram_data * histogram_get_data_global(bool scale);
histogram_data * histogram_get_data_db(bool scale, Oid databaseoid);
histogram_data * histogram_get_data_dbs(bool scale, int * dbcount);

/* reset the shared segment */
void histogram_reset(bool locked, bool remove);     /* all histograms (optionally reset list of dbs) */
void histogram_reset_global(bool locked);           /* just the global histogram */
bool histogram_reset_db(bool locked, Oid databaseoid, bool remove);    /* a histogram for a DB with given OID */

/* version of the last segment change */
int64 histogram_version(void); /* global */

/* get timestamp of the last reset of a histogram */
TimestampTz histogram_get_reset_global(void);                       /* global histogram */
TimestampTz histogram_get_reset_db(Oid databaseoid, bool * found);  /* per-database histograms */