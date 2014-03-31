#include <stdio.h>
#include <stdlib.h>
#include <sys/shm.h>
#include <sys/stat.h>

#include <sys/types.h>
#include <sys/ipc.h>

#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/fd.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "utils/guc.h"
#include "tcop/utility.h"

#include "libpq/md5.h"

#include "queryhist.h"

#define DB_NOT_FOUND    (-1)

/* is this a linear (bins of equal width) or logarithmic histogram? */
static const struct config_enum_entry histogram_type_options[] = {
    {"linear", HISTOGRAM_LINEAR, false},
    {"log", HISTOGRAM_LOG, false},
    {NULL, 0, false}
};

static int nesting_level = 0;

/* private functions */
static void histogram_shmem_startup(void);
static void histogram_shmem_shutdown(int code, Datum arg);

static void histogram_load_from_file(void);

#if (PG_VERSION_NUM >= 90100)
static void set_histogram_bins_count_hook(int newval, void *extra);
static void set_histogram_bins_width_hook(int newval, void *extra);
static void set_histogram_sample_hook(int newval, void *extra);
static void set_histogram_type_hook(int newval, void *extra);
static void set_histogram_track_utility(bool newval, void *extra);
static void set_max_databases_hook(int newval, void *extra);
#else
static bool set_histogram_bins_count_hook(int newval, bool doit, GucSource source);
static bool set_histogram_bins_width_hook(int newval, bool doit, GucSource source);
static bool set_histogram_sample_hook(int newval, bool doit, GucSource source);
static bool set_histogram_type_hook(int newval, bool doit, GucSource source);
static bool set_histogram_track_utility(bool newval, bool doit, GucSource source);
static bool set_max_databases_hook(int newval, bool doit, GucSource source);
#endif

/* return from a hook */
#if (PG_VERSION_NUM >= 90100)
#define HOOK_RETURN(a)	return;
#else
#define HOOK_RETURN(a)	return (a);
#endif

static void add_query(time_bin_t duration);
static bool query_histogram_enabled(void);
static int  query_bin(int bins, int step, time_bin_t duration);
static int  find_histogram_index(Oid databaseoid);

/* The histogram itself is stored in a shared memory segment
 * with this layout (see the histogram_info_t below).
 *
 * - bins (int => 4B)
 * - step (int => 4B)
 * - type (int => 4B)
 * - sample (int => 4B)
 *
 * - count bins (HIST_BINS_MAX+1) x sizeof(unsigned long)
 * - time  bins (HIST_BINS_MAX+1) x sizeof(unsigned long)
 *
 * This segment is initialized in the first process that accesses it (see
 * histogram_shmem_startup function).
 */
#define SEGMENT_NAME    "query_histogram"

/* default values (used for init) */
static bool default_histogram_dynamic = false;
static bool default_histogram_utility = true; /* track DDL */
static int  default_histogram_bins = 100;
static int  default_histogram_step = 100;
static int  default_histogram_sample_pct = 100;
static int  default_histogram_type = HISTOGRAM_LINEAR;

/* we'll allocate histograms for 100 databases by default - if we have more
 * databases, this needs to be increased (the matching GUC) */
static int  max_database_histograms = 5;

/* set at the end of init */
static bool histogram_is_dynamic = true;

/* TODO It might be useful to allow 'per database' histograms, or to collect
 *      the data only for some of the databases. So there might be options
 *
 *        query_histogram.per_database={true|false}
 *        query_histogram.databases= ... list of database names
 *
 *      and the per_database would require databases (so that we know how
 *      much memory to allocate etc.)
 */

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

void        _PG_init(void);
void        _PG_fini(void);

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
                    ScanDirection direction,
                    long count);
static void explain_ExecutorEnd(QueryDesc *queryDesc);

#if (PG_VERSION_NUM >= 90300)
static void queryhist_ProcessUtility(Node *parsetree, const char *queryString,
                                     ProcessUtilityContext context,
                                     ParamListInfo params, DestReceiver *dest,
                                     char *completionTag);
#else
static void queryhist_ProcessUtility(Node *parsetree,
                                     const char *queryString, ParamListInfo params,
                                     bool isTopLevel, DestReceiver *dest,
                                     char *completionTag);
#endif

#if (PG_VERSION_NUM >= 90100)
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static void explain_ExecutorFinish(QueryDesc *queryDesc);
#endif

/* the whole histogram (info and data) */
static segment_info_t * segment_info = NULL;

/* caching db index lookup (until the segment version changes) */
static int64    lookup_version = -1; /* version when the lookup was performed */
static int      lookup_db_index = DB_NOT_FOUND;


/*
 * Module load callback
 */
void
_PG_init(void)
{

    /* */
    if (!process_shared_preload_libraries_in_progress)
        return;

    /* Define custom GUC variables. */
    DefineCustomBoolVariable("query_histogram.dynamic",
                              "Dynamic histograms may be modified on the fly (to some extent).",
                             NULL,
                             &default_histogram_dynamic,
                             false,
                             PGC_BACKEND,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             NULL,
                             NULL);

    /* Define custom GUC variables. */
    DefineCustomBoolVariable("query_histogram.track_utility",
                              "Selects whether utility commands are tracked.",
                             NULL,
                             &default_histogram_utility,
                             true,
                             PGC_SUSET,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             &set_histogram_track_utility,
                             NULL);

    DefineCustomIntVariable("query_histogram.bin_count",
                         "Sets the number of bins of the histogram.",
                         "Zero disables collecting the histogram.",
                            &default_histogram_bins,
                            100,
                            0, 1000,
                            PGC_SUSET,
                            0,
#if (PG_VERSION_NUM >= 90100)
                            NULL,
#endif
                            &set_histogram_bins_count_hook,
                            NULL);

    DefineCustomIntVariable("query_histogram.bin_width",
                         "Sets the width of the histogram bin.",
                            NULL,
                            &default_histogram_step,
                            100,
                            1, 1000,
                            PGC_SUSET,
                            GUC_UNIT_MS,
#if (PG_VERSION_NUM >= 90100)
                            NULL,
#endif
                            &set_histogram_bins_width_hook,
                            NULL);

    DefineCustomIntVariable("query_histogram.sample_pct",
                         "What portion of the queries should be sampled (in percent).",
                            NULL,
                            &default_histogram_sample_pct,
                            5,
                            1, 100,
                            PGC_SUSET,
                            0,
#if (PG_VERSION_NUM >= 90100)
                            NULL,
#endif
                            &set_histogram_sample_hook,
                            NULL);

    DefineCustomEnumVariable("query_histogram.histogram_type",
                             "Type of the histogram (how the bin width is computed).",
                             NULL,
                             &default_histogram_type,
                             HISTOGRAM_LINEAR,
                             histogram_type_options,
                             PGC_SUSET,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             &set_histogram_type_hook,
                             NULL);

    DefineCustomIntVariable("query_histogram.max_databases",
                            "Max number of databases tracked (max histograms).",
                            NULL,
                            &max_database_histograms,
                            100,
                            0, INT_MAX,
                            PGC_SUSET,
                            0,
#if (PG_VERSION_NUM >= 90100)
                            NULL,
#endif
                            &set_max_databases_hook,
                            NULL);

    EmitWarningsOnPlaceholders("query_histogram");

    /*
     * Request additional shared resources.  (These are no-ops if we're not in
     * the postmaster process.)  We'll allocate or attach to the shared
     * resources in histogram_shmem_startup().
     */
    RequestAddinShmemSpace(histogram_segment_size(max_database_histograms));
    RequestAddinLWLocks(histogram_segment_locks(max_database_histograms));

    /* Install hooks. */
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = histogram_shmem_startup;

    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = explain_ExecutorStart;
    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = explain_ExecutorRun;
#if (PG_VERSION_NUM >= 90100)
    prev_ExecutorFinish = ExecutorFinish_hook;
    ExecutorFinish_hook = explain_ExecutorFinish;
#endif
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = explain_ExecutorEnd;
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = queryhist_ProcessUtility;

}


/*
 * Module unload callback
 */
void
_PG_fini(void)
{
    /* Uninstall hooks. */
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorRun_hook = prev_ExecutorRun;
#if (PG_VERSION_NUM >= 90100)
    ExecutorFinish_hook = prev_ExecutorFinish;
#endif
    ExecutorEnd_hook = prev_ExecutorEnd;
    shmem_startup_hook = prev_shmem_startup_hook;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{

    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    /* Enable the histogram whenever the histogram is dynamic or (bins>0). */
    if (query_histogram_enabled())
    {
        /*
         * Set up to track total elapsed time in ExecutorRun.  Make sure the
         * space is allocated in the per-query context so it will go away at
         * ExecutorEnd.
         */
        if (queryDesc->totaltime == NULL)
        {
            MemoryContext oldcxt;

            oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
            queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
            MemoryContextSwitchTo(oldcxt);
        }
    }
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorRun)
            prev_ExecutorRun(queryDesc, direction, count);
        else
            standard_ExecutorRun(queryDesc, direction, count);
        nesting_level--;
    }
    PG_CATCH();
    {
        nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}

#if (PG_VERSION_NUM >= 90100)
/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
explain_ExecutorFinish(QueryDesc *queryDesc)
{
    nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorFinish)
            prev_ExecutorFinish(queryDesc);
        else
            standard_ExecutorFinish(queryDesc);
        nesting_level--;
    }
    PG_CATCH();
    {
        nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}
#endif

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
    if (queryDesc->totaltime && (nesting_level == 0) && query_histogram_enabled())
    {
        float seconds;

        /*
         * Make sure stats accumulation is done.  (Note: it's okay if several
         * levels of hook all do this.)
         */
        InstrEndLoop(queryDesc->totaltime);

        /* Log plan if duration is exceeded. */
        seconds = queryDesc->totaltime->total;

        /* is the histogram static or dynamic? */
        if (! default_histogram_dynamic) {

            /* in case of static histogram, it's quite simple - check the number
             * of bins and a sample rate - then lock the segment, add the query
             * and unlock it again */
            if ((default_histogram_bins > 0) && (rand() % 100 <  default_histogram_sample_pct)) {
                LWLockAcquire(segment_info->lock, LW_SHARED);
                add_query(seconds);
                LWLockRelease(segment_info->lock);
            }

        } else {
            /* when the histogram is dynamic, we have to lock it first, as we
             * will access the sample_pct in the histogram */
            LWLockAcquire(segment_info->lock, LW_SHARED);
            if ((segment_info->bins > 0) && (rand() % 100 <  segment_info->sample_pct)) {
                add_query(seconds);
            }
            LWLockRelease(segment_info->lock);

        }

    }

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);

}

/*
 * ProcessUtility hook (API changed in 9.3)
 */
static void
#if (PG_VERSION_NUM >= 90300)
queryhist_ProcessUtility(Node *parsetree, const char *queryString,
                         ProcessUtilityContext context, ParamListInfo params,
                         DestReceiver *dest, char *completionTag)
#else
queryhist_ProcessUtility(Node *parsetree, const char *queryString,
                         ParamListInfo params, bool isTopLevel,
                         DestReceiver *dest, char *completionTag)
#endif
{
    if (default_histogram_utility && (nesting_level == 0) && query_histogram_enabled())
    {
        /* collecting histogram is enabled, we're in top level (nesting_level=0) */
        instr_time  start;
        instr_time  duration;
        float       seconds;

        INSTR_TIME_SET_CURRENT(start);

        nesting_level++;
        PG_TRY();
        {
            if (prev_ProcessUtility)
#if (PG_VERSION_NUM >= 90300)
                prev_ProcessUtility(parsetree, queryString, context, params,
                                    dest, completionTag);
#else
                prev_ProcessUtility(parsetree, queryString, params,
                                    isTopLevel, dest, completionTag);
#endif
            else
#if (PG_VERSION_NUM >= 90300)
                standard_ProcessUtility(parsetree, queryString, context, params,
                                        dest, completionTag);
#else
                standard_ProcessUtility(parsetree, queryString, params,
                                        isTopLevel, dest, completionTag);
#endif

            nesting_level--;
        }
        PG_CATCH();
        {
            nesting_level--;
            PG_RE_THROW();
        }
        PG_END_TRY();

        INSTR_TIME_SET_CURRENT(duration);
        INSTR_TIME_SUBTRACT(duration, start);

        seconds = INSTR_TIME_GET_DOUBLE(duration);

        /* is the histogram static or dynamic? */
        if (! default_histogram_dynamic) {

            /* in case of static histogram, it's quite simple - check the number
             * of bins and a sample rate - then lock the segment, add the query
             * and unlock it again */
            if ((default_histogram_bins > 0) && (rand() % 100 <  default_histogram_sample_pct)) {
                LWLockAcquire(segment_info->lock, LW_SHARED);
                add_query(seconds);
                LWLockRelease(segment_info->lock);
            }

        } else {
            /* when the histogram is dynamic, we have to lock it first, as we
             * will access the sample_pct in the histogram */
            LWLockAcquire(segment_info->lock, LW_SHARED);
            if ((segment_info->bins > 0) && (rand() % 100 <  segment_info->sample_pct)) {
                add_query(seconds);
            }
            LWLockRelease(segment_info->lock);

        }

    }
    else
    {
        /* collecting histogram is not enabled, so just call the hooks directly */
        if (prev_ProcessUtility)
#if (PG_VERSION_NUM >= 90300)
            prev_ProcessUtility(parsetree, queryString, context, params,
                                dest, completionTag);
#else
            prev_ProcessUtility(parsetree, queryString, params,
                                isTopLevel, dest, completionTag);
#endif
            else
#if (PG_VERSION_NUM >= 90300)
            standard_ProcessUtility(parsetree, queryString, context, params,
                                    dest, completionTag);
#else
            standard_ProcessUtility(parsetree, queryString, params,
                                    isTopLevel, dest, completionTag);
#endif
    }
}


/* This is probably the most important part - allocates the shared
 * segment, initializes it etc. */
static
void histogram_shmem_startup() {

    int i;
    bool found = FALSE;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    /*
     * Create or attach to the shared memory state, including hash table
     */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    segment_info = ShmemInitStruct(SEGMENT_NAME,
                    histogram_segment_size(max_database_histograms),
                    &found);

    elog(LOG, "query histogram segment init size: %lu B",
         histogram_segment_size(max_database_histograms));

    if (! found) {

        /* First time through ... */
        segment_info->lock = LWLockAssign();

        segment_info->type = default_histogram_type;
        segment_info->bins = default_histogram_bins;
        segment_info->step = default_histogram_step;
        segment_info->sample_pct = default_histogram_sample_pct;
        segment_info->max_databases = max_database_histograms;
        segment_info->version = 0;

        /* list of databases is right after the structure */
        segment_info->databases = (db_info_t*)((char*)segment_info + sizeof(segment_info_t));

        /* list of histograms is right after the list of databases */
        segment_info->histograms = (histogram_info_t*)((char*)segment_info->databases
                                    + (max_database_histograms * sizeof(db_info_t)));

        /* reset the structures */
        memset(segment_info->databases,     0, (max_database_histograms * sizeof(db_info_t)));
        memset(segment_info->histograms,    0, (max_database_histograms + 1) * sizeof(histogram_info_t));

        /* allocate the per-database locks */
        for (i = 0; i < (max_database_histograms+1); i++)
            segment_info->histograms[i].lock = LWLockAssign();

        /* OK, done */
        elog(DEBUG1, "shared memory segment (query histogram) successfully created");

    }

    LWLockRelease(AddinShmemInitLock);

    /*
     * If we're in the postmaster (or a standalone backend...), set up a shmem
     * exit hook to dump the statistics to disk.
     */
    if (!IsUnderPostmaster)
        on_shmem_exit(histogram_shmem_shutdown, (Datum) 0);

    if (! found)
        histogram_load_from_file();

    histogram_is_dynamic = default_histogram_dynamic;

    /* seed the random generator */
    // srand((int)segment_info);

}

/* Loads the histogram data from a file (and checks that the md5 hash of the contents matches). */
static
void histogram_load_from_file(void) {

    FILE * file;
    char hash_file[16];
    char hash_comp[16];

    int32 segment_length = 0;
    char * segment_buffer = NULL;   /* segment_info_t with database/histogram data */

    segment_info_t * segment_info_tmp;

    int i, j;

    /* load the histogram from the file */
    file = AllocateFile(HISTOGRAM_DUMP_FILE, PG_BINARY_R);

    if ((file == NULL) && (errno == ENOENT)) {
        elog(LOG, "query histogram dump file '%s' not found", HISTOGRAM_DUMP_FILE);
        return;
    } else if (file == NULL) {
        ereport(LOG,
                (errcode_for_file_access(),
                errmsg("unable to open histogram file \"%s\": %m",
                        HISTOGRAM_DUMP_FILE)));
        goto error;
    }

    /* read the first 16 bytes (should be a MD5 hash of the segment) */
    if (fread(hash_file, 16, 1, file) != 1) {
        ereport(LOG,
                (errcode_for_file_access(),
                errmsg("could not read MD5 hash from the histogram file \"%s\": %m",
                        HISTOGRAM_DUMP_FILE)));
        goto error;
    }

    /* read the next 4 bytes (should be a length the segment data) */
    if (fread(&segment_length, 4, 1, file) != 1) {
        ereport(LOG,
                (errcode_for_file_access(),
                errmsg("could not read data length from the histogram file \"%s\": %m",
                        HISTOGRAM_DUMP_FILE)));
        goto error;
    }

    /* read the segment including database / histogram data */
    segment_buffer = palloc0(segment_length);
    if (fread(segment_buffer, segment_length, 1, file) != 1)
        goto error;

    /* compute md5 hash of the buffer */
    pg_md5_binary(segment_buffer, segment_length, hash_comp);

    /* check that the hashes are equal (the file is not corrupted) */
    if (memcmp(hash_file, hash_comp, 16) == 0) {

        /* now we know the buffer contains 'valid' histogram data */
        segment_info_tmp = (segment_info_t*)segment_buffer;

        /* we can copy it into the shared segment iff the histogram is static and has the same
         * parameters, or if it's dynamic (in this case the parameters may be arbitrary)
         *
         * FIXME check that we have enough memory
         */
        if ((default_histogram_dynamic) ||
            ((! default_histogram_dynamic) && ((segment_info_tmp->bins == default_histogram_bins)
                                            && (segment_info_tmp->step == default_histogram_step)
                                            && (segment_info_tmp->sample_pct == default_histogram_sample_pct)
                                            && (segment_info_tmp->type == default_histogram_type)
                                            && (segment_info_tmp->current_databases <= max_database_histograms)))) {

            /* we must not mess with the LW locks, so we'll just copy the histogram data (we can't mess with the locks) */

            /* fix structure of the new segment (pointers to databases and histograms) */
            segment_info_tmp->databases = (db_info_t*)((char*)segment_info_tmp + sizeof(segment_info_t));

            /* list of histograms is right after the list of databases */
            segment_info_tmp->histograms = (histogram_info_t*)((char*)segment_info_tmp->databases +
                                            (segment_info_tmp->max_databases * sizeof(db_info_t)));

            /* copy the database info */
            for (i = 0; i < segment_info_tmp->current_databases; i++) {
                segment_info->databases[i].databaseoid = segment_info_tmp->databases[i].databaseoid;
                segment_info->databases[i].histogram_idx = segment_info_tmp->databases[i].histogram_idx;
            }

            /* copy the histogram data (but only for the databases in the file) */
            for (i = 0; i <= segment_info_tmp->current_databases; i++) {

                segment_info->histograms[i].last_reset = segment_info_tmp->histograms[i].last_reset;

                for (j = 0; j < HIST_BINS_MAX+1; j++) {
                    segment_info->histograms[i].count_bins[j] = segment_info_tmp->histograms[i].count_bins[j];
                    segment_info->histograms[i].time_bins[j]  = segment_info_tmp->histograms[i].time_bins[j];
                }
            }

            /* copy the values from the histogram */
            default_histogram_type = segment_info->type;
            default_histogram_bins = segment_info->bins;
            default_histogram_step = segment_info->step;
            default_histogram_sample_pct = segment_info->sample_pct;

            segment_info->current_databases = segment_info_tmp->current_databases;

            elog(DEBUG1, "successfully loaded query histogram from a file : %s",
                HISTOGRAM_DUMP_FILE);

        } else {

            elog(WARNING, "can't load the histogram from '%s' because of parameter values differences",
                 HISTOGRAM_DUMP_FILE);

        }

    } else {
        elog(WARNING, "can't load the histogram from %s because the hash is incorrect",
             HISTOGRAM_DUMP_FILE);
    }

    FreeFile(file);
    pfree(segment_buffer);

    return;

error: /* error handling */
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not read query_histogram file \"%s\": %m",
                    HISTOGRAM_DUMP_FILE)));
    if (segment_buffer)
        pfree(segment_buffer);
    if (file)
        FreeFile(file);

}

/* Dumps the histogram data into a file (with a md5 hash of the contents at the beginning). */
static
void histogram_shmem_shutdown(int code, Datum arg) {

    FILE   *file;
    char    buffer[16];
    int32   length;

    file = AllocateFile(HISTOGRAM_DUMP_FILE, PG_BINARY_W);
    if (file == NULL) {
        ereport(LOG,
                (errcode_for_file_access(),
                errmsg("unable to open query histogram file \"%s\": %m",
                        HISTOGRAM_DUMP_FILE)));
        goto error;
    }

    /* compute amount of histogram data to write to the file */
    length = histogram_segment_size(segment_info->max_databases);

    elog(DEBUG1, "writing %d bytes into query histogram file", length);

    /* lets compute MD5 hash of the shared memory segment and write it to the file */
    pg_md5_binary(segment_info, length, buffer);

    if (fwrite(buffer, 16, 1, file) != 1) {
        ereport(LOG,
                (errcode_for_file_access(),
                errmsg("unable to write MD5 hash to the histogram file \"%s\": %m",
                        HISTOGRAM_DUMP_FILE)));
        goto error;
    }

    /* now write the length to the file */

    if (fwrite(&length, 4, 1, file) != 1) {
        ereport(LOG,
                (errcode_for_file_access(),
                errmsg("unable to write segment size to the histogram file \"%s\": %m",
                        HISTOGRAM_DUMP_FILE)));
        goto error;
    }

    /* and finally write the actual histogram data */
    if (fwrite(segment_info, length, 1, file) != 1)
        goto error;

    FreeFile(file);

    return;

error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not write query histogram file \"%s\": %m",
                    HISTOGRAM_DUMP_FILE)));
    if (file)
        FreeFile(file);

}

/* resets all the histograms (global and per-database histograms) */
void histogram_reset(bool locked, bool remove) {

    int i;
    TimestampTz now = GetCurrentTimestamp();

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    if (! locked)
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);

    for (i = 0; i <= segment_info->current_databases; i++) {
        
        if (remove)
            segment_info->databases[i].databaseoid = InvalidOid;
        
        memset(segment_info->histograms[i].count_bins, 0, (HIST_BINS_MAX+1)*sizeof(count_bin_t));
        memset(segment_info->histograms[i].time_bins,  0, (HIST_BINS_MAX+1)*sizeof(time_bin_t));
        segment_info->histograms[i].last_reset = now;
    }

    segment_info->version++;

    /* if it was not locked before, we can release the lock now */
    if (! locked)
        LWLockRelease(segment_info->lock);

}

/* resets the global histogram */
void histogram_reset_global(bool locked) {

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    if (! locked)
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);

    memset(segment_info->histograms[0].count_bins, 0, (HIST_BINS_MAX+1)*sizeof(count_bin_t));
    memset(segment_info->histograms[0].time_bins,  0, (HIST_BINS_MAX+1)*sizeof(time_bin_t));
    segment_info->histograms[0].last_reset = GetCurrentTimestamp();

    segment_info->version++;

    /* if it was not locked before, we can release the lock now */
    if (! locked)
        LWLockRelease(segment_info->lock);

}

/* resets a histogram for a DB with the given OID (and optionally remove the database from
 * the list of list of tracked databases) */
bool histogram_reset_db(bool locked, Oid databaseoid, bool remove) {

    bool reset = FALSE;
    int db_index;

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    if (! locked)
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);

    db_index = find_histogram_index(databaseoid);

    if (db_index != DB_NOT_FOUND) {

        /* OK, we've found the DB */
        reset = TRUE;

        /* if removing the database, we'll simply replace it with the last database
         * (unless it's the last one, of course - in that case we'll just zero it) */
        if (remove) {

            /* not the last DB - replace it with the last one */
            if (db_index != (segment_info->current_databases-1)) {

                /* set the OID */
                segment_info->databases[db_index].databaseoid
                    = segment_info->databases[segment_info->current_databases-1].databaseoid;

                /* copy the histogram data */
                memcpy(segment_info->histograms[db_index].count_bins,
                       segment_info->histograms[segment_info->current_databases-1].count_bins,
                       (HIST_BINS_MAX+1)*sizeof(count_bin_t));
                memcpy(segment_info->histograms[db_index].time_bins,
                       segment_info->histograms[segment_info->current_databases-1].time_bins,
                       (HIST_BINS_MAX+1)*sizeof(time_bin_t));

            }

            /* zero the last database (and remove it */
            memset(segment_info->histograms[segment_info->current_databases-1].count_bins, 0,
                   (HIST_BINS_MAX+1)*sizeof(count_bin_t));
            memset(segment_info->histograms[segment_info->current_databases-1].time_bins,  0,
                   (HIST_BINS_MAX+1)*sizeof(time_bin_t));

            /* remove the last database */
            segment_info->current_databases--;

            /* we've changed the segment-level info (list of databases) */
            segment_info->version++;

        } else {

            /* meh, just zero it */
            memset(segment_info->histograms[db_index].count_bins, 0, (HIST_BINS_MAX+1)*sizeof(count_bin_t));
            memset(segment_info->histograms[db_index].time_bins,  0, (HIST_BINS_MAX+1)*sizeof(time_bin_t));

            /* no need to set the segment_info.last_change timestamp here */
            segment_info->histograms[db_index].last_reset = GetCurrentTimestamp();

        }
    }

    /* if it was not locked before, we can release the lock now */
    if (! locked)
        LWLockRelease(segment_info->lock);

    /* TRUE - we've found the DB */
    return reset;

}

/* assumes the segment is already locked in LW_SHARED mode */
static
void add_query(time_bin_t duration) {

    int db_index, current_dbs;
    int bin = query_bin(segment_info->bins, segment_info->step, duration);

    /* global histogram - lock in exclusive mode */
    LWLockAcquire(segment_info->histograms[0].lock, LW_EXCLUSIVE);
    segment_info->histograms[0].count_bins[bin] += 1;
    segment_info->histograms[0].time_bins[bin] += duration;
    LWLockRelease(segment_info->histograms[0].lock);

    /* look through the databases */
    db_index = find_histogram_index(MyDatabaseId);

    /* TODO The db index may be cached in the backend (and then rechecked). */

    /* not found but we have enough space for another database */
    if ((db_index == DB_NOT_FOUND) &&
        (segment_info->current_databases < segment_info->max_databases)) {

        /* keep number of DBs (unless it changes, we don't need to repeat the search) */
        current_dbs = segment_info->current_databases;

        /* relock in LW_EXCLUSIVE */
        LWLockRelease(segment_info->lock);
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);

        /* someone else added a database -> repeat the search */
        if (current_dbs != segment_info->current_databases)
            db_index = find_histogram_index(MyDatabaseId);

        /* still not found -> add the DB if enough space */
        if ((db_index == DB_NOT_FOUND) &&
            (segment_info->current_databases < segment_info->max_databases)) {

            /* store the index of the database */
            db_index = segment_info->current_databases;

            /* store OID of the database */
            segment_info->databases[segment_info->current_databases].databaseoid
                = MyDatabaseId;

            /* index of the first free histogram is (1 + # of databases) */
            segment_info->databases[segment_info->current_databases].histogram_idx
                = segment_info->current_databases + 1;

            /* increment # of databases and segment version */
            segment_info->current_databases++;
            segment_info->version++;

        }

        /* we'll keep the LW_EXCLUSIVE lock - maybe that could be improved (but then
         * we'd have to worry that the database we just inserted might disappear) */

    }

    /* so, do we have a matching database? */
    if (db_index != DB_NOT_FOUND) {

        int histogram_index = segment_info->databases[db_index].histogram_idx;

        segment_info->histograms[histogram_index].count_bins[bin] += 1;
        segment_info->histograms[histogram_index].time_bins[bin] += duration;

    }

}

/* Searches the list of tracked databases (using the OID). If the  database is
 * already tracked, this returns index within the array of databases (which is
 * valid as long as the LW lock on segment_info_t is held).
 *
 * TODO This needs to be optimized to work well with large number od databases.
 * Currently it does a linear search for every query. One option is to sort the
 * databases and perform a binary search, another option is caching using the
 * last_change field. Or maybe both.
 *
 * Some of the code currently assumes the ordering of databases and histograms
 * matches (instead of using db_info_t.histogram_idx). If this changes (e.g.
 * because of switching to binary search), this needs to be fixed carefully.
 *
 * This assumes the segment is already locked (at least in LW_SHARED mode).
 */
static
int find_histogram_index(Oid databaseoid) {

    int i;

    /* if the version did not change, return the cached index */
    if (lookup_version == segment_info->version)
        return lookup_db_index;

    /* ok, either version change or first lookup - first store the current version
     * and reset the index */
    lookup_db_index = DB_NOT_FOUND;
    lookup_version = segment_info->version;

    /* walk through list of databases, compre oids */
    for (i = 0; i < segment_info->current_databases; i++)
        if (segment_info->databases[i].databaseoid == databaseoid) {
            lookup_db_index = i;
            break;
        }

    /* nope, we haven't found the database */
    return lookup_db_index;

}

/* Computes index of the bin the query belongs to (given duration). */
static
int query_bin(int bins, int step, time_bin_t duration) {

    int bin = 0;

    if (segment_info->type == HISTOGRAM_LINEAR)
        bin = (int)floor((duration * 1000.0) / (segment_info->step));
    else
        bin = (int)floor(log2(1 + ((duration * 1000.0) / (segment_info->step))));

    /* queries that take longer than the last bin should go to
     * the (HIST_BINS_MAX+1) bin */
    return (bin >= (segment_info->bins)) ? (segment_info->bins) : bin;

}

/* Returns version of the last segment change (adding a new database to the list,
 * removing one etc.) */
int64 histogram_version() {

    int64 version;

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    LWLockAcquire(segment_info->lock, LW_SHARED);
    version = segment_info->version;
    LWLockRelease(segment_info->lock);

    return version;

}

/* Returns timestamp of tha last reset for global histogram. */
TimestampTz histogram_get_reset_global(void) {

    TimestampTz timestamp;

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    /* lock the segment / histogram (in shared mode) */
    LWLockAcquire(segment_info->lock, LW_SHARED);
    LWLockAcquire(segment_info->histograms[0].lock, LW_SHARED);

    timestamp = segment_info->histograms[0].last_reset;

    LWLockRelease(segment_info->histograms[0].lock);
    LWLockRelease(segment_info->lock);

    return timestamp;

}

/* Returns timestamp of tha last reset for global histogram. */
TimestampTz histogram_get_reset_db(Oid databaseoid, bool * found) {

    int db_index;
    TimestampTz timestamp = 0;

    *found = TRUE; /* not found by default */

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    LWLockAcquire(segment_info->lock, LW_SHARED);

    db_index = find_histogram_index(databaseoid);

    if (db_index != DB_NOT_FOUND) {
        LWLockAcquire(segment_info->histograms[db_index].lock, LW_SHARED);
        timestamp = segment_info->histograms[db_index].last_reset;
        LWLockRelease(segment_info->histograms[db_index].lock);
        *found = TRUE;
    }

    LWLockRelease(segment_info->lock);

    return timestamp;

}

/* returns the global histogram */
histogram_data * histogram_get_data_global(bool scale) {

    int i = 0;
    double coeff = 0;
    histogram_data * tmp = NULL;

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    tmp = (histogram_data *)palloc(sizeof(histogram_data));

    memset(tmp, 0, sizeof(histogram_data));

    /* we can do this using a shared lock */
    LWLockAcquire(segment_info->lock, LW_SHARED);

    tmp->histogram_type = (segment_info->type);
    tmp->bins_count = (segment_info->bins);
    tmp->bins_width = (segment_info->step);

    if (segment_info->bins > 0) {

        tmp->count_data = (count_bin_t *) palloc(sizeof(count_bin_t) * (segment_info->bins+1));
        tmp->time_data  =  (time_bin_t *) palloc(sizeof(time_bin_t)  * (segment_info->bins+1));

        memcpy(tmp->count_data, segment_info->histograms[0].count_bins, sizeof(count_bin_t) * (segment_info->bins+1));
        memcpy(tmp->time_data,  segment_info->histograms[0].time_bins,  sizeof(time_bin_t)  * (segment_info->bins+1));

        /* check if we need to scale the histogram (to compensate for the sampling rate,
         * which is the desired behavior most of the time) */
        if (scale && (segment_info->sample_pct < 100)) {
            coeff = (100.0 / (segment_info->sample_pct));
            for (i = 0; i < (segment_info->bins+1); i++) {
                tmp->count_data[i] = tmp->count_data[i] * coeff;
                tmp->time_data[i]  = tmp->time_data[i] * coeff;
            }
        }

        /* compute totals, to allow easy evaluation of percentages */
        for (i = 0; i < (segment_info->bins+1); i++) {
            tmp->total_count += tmp->count_data[i];
            tmp->total_time  += tmp->time_data[i];
        }

    }

    /* release the lock */
    LWLockRelease(segment_info->lock);

    return tmp;

}

/* returns the histogram for given database */
histogram_data * histogram_get_data_db(bool scale, Oid databaseoid) {

    int i = 0;
    double coeff = 0;
    histogram_data * tmp = NULL;
    int db_index;

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    /* we can do this using a shared lock */
    LWLockAcquire(segment_info->lock, LW_SHARED);

    /* lookup index of the histogram for this database */
    db_index = find_histogram_index(databaseoid);

    /* not found */
    if (db_index == DB_NOT_FOUND)
        return NULL;

    /* allocate the data structure */

    tmp = (histogram_data *)palloc(sizeof(histogram_data));

    memset(tmp, 0, sizeof(histogram_data));

    tmp->histogram_type = (segment_info->type);
    tmp->bins_count = (segment_info->bins);
    tmp->bins_width = (segment_info->step);
    tmp->databaseoid = databaseoid;

    if (segment_info->bins > 0) {

        tmp->count_data = (count_bin_t *) palloc(sizeof(count_bin_t) * (segment_info->bins+1));
        tmp->time_data  =  (time_bin_t *) palloc(sizeof(time_bin_t)  * (segment_info->bins+1));

        /* first histogram is the 'global' one */
        memcpy(tmp->count_data, segment_info->histograms[db_index+1].count_bins, sizeof(count_bin_t) * (segment_info->bins+1));
        memcpy(tmp->time_data,  segment_info->histograms[db_index+1].time_bins,  sizeof(time_bin_t)  * (segment_info->bins+1));

        /* check if we need to scale the histogram (to compensate for the sampling rate,
         * which is the desired behavior most of the time) */
        if (scale && (segment_info->sample_pct < 100)) {
            coeff = (100.0 / (segment_info->sample_pct));
            for (i = 0; i < (segment_info->bins+1); i++) {
                tmp->count_data[i] = tmp->count_data[i] * coeff;
                tmp->time_data[i]  = tmp->time_data[i] * coeff;
            }
        }

        /* compute totals, to allow easy evaluation of percentages */
        for (i = 0; i < (segment_info->bins+1); i++) {
            tmp->total_count += tmp->count_data[i];
            tmp->total_time  += tmp->time_data[i];
        }

    }

    /* release the lock */
    LWLockRelease(segment_info->lock);

    return tmp;

}

/* returns histograms for all databases (optionally including the global histogram) */
histogram_data * histogram_get_data_dbs(bool scale, int * dbcount) {

    int i = 0, j = 0;
    double coeff = 0;

    histogram_data * tmp = NULL;
    histogram_data * prev = NULL;

    if (! segment_info) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("query_histogram must be loaded via shared_preload_libraries")));
    }

    /* we can do this using a shared lock */
    LWLockAcquire(segment_info->lock, LW_SHARED);

    /* loop through the databases */
    for (j = 0; j <= segment_info->current_databases; j++) {

        /* keep the link */
        prev = tmp;

        /* allocate the data structure */
        tmp = (histogram_data *)palloc(sizeof(histogram_data));

        memset(tmp, 0, sizeof(histogram_data));

        tmp->histogram_type = (segment_info->type);
        tmp->bins_count = (segment_info->bins);
        tmp->bins_width = (segment_info->step);
        tmp->databaseoid = (j == 0) ? InvalidOid : segment_info->databases[j-1].databaseoid;
        tmp->next = prev; /* link to the next set */

        if (segment_info->bins > 0) {

            tmp->count_data = (count_bin_t *) palloc(sizeof(count_bin_t) * (segment_info->bins+1));
            tmp->time_data  =  (time_bin_t *) palloc(sizeof(time_bin_t)  * (segment_info->bins+1));

            /* first histogram is 'global' so skip it (and then current_databases items) */
            memcpy(tmp->count_data, segment_info->histograms[j].count_bins, sizeof(count_bin_t) * (segment_info->bins+1));
            memcpy(tmp->time_data,  segment_info->histograms[j].time_bins,  sizeof(time_bin_t)  * (segment_info->bins+1));

            /* check if we need to scale the histogram (to compensate for the sampling rate,
             * which is the desired behavior most of the time) */
            if (scale && (segment_info->sample_pct < 100)) {
                coeff = (100.0 / (segment_info->sample_pct));
                for (i = 0; i < (segment_info->bins+1); i++) {
                    tmp->count_data[i] = tmp->count_data[i] * coeff;
                    tmp->time_data[i]  = tmp->time_data[i] * coeff;
                }
            }

            /* compute totals, to allow easy evaluation of percentages */
            for (i = 0; i < (segment_info->bins+1); i++) {
                tmp->total_count += tmp->count_data[i];
                tmp->total_time  += tmp->time_data[i];
            }
        }
    }

    /* add 1 if including global */
    *dbcount = segment_info->current_databases + 1;

    /* release the lock */
    LWLockRelease(segment_info->lock);

    return tmp;

}

/* GUC hooks */

#if (PG_VERSION_NUM >= 90100)
static void set_histogram_bins_count_hook(int newval, void *extra) {
#else
static bool set_histogram_bins_count_hook(int newval, bool doit, GucSource source) {
#endif

    if (! histogram_is_dynamic) {
        elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
                      "it's not possible to change the number of bins.");

        HOOK_RETURN(false);
    }

    if (segment_info) {
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);

        /* if the histogram is logarithmic, there really is not much point
         * in sending more than 32 bins (or something like that) */
        if (segment_info->type == HISTOGRAM_LOG) {
            int max_count = (int)ceil(log2(INT_MAX/segment_info->step));
            if (newval > max_count) {
                elog(NOTICE, "the max bin count %d is too high for log histogram with "
                "%d ms resolution, using %d", newval, segment_info->step, max_count);
                newval = max_count;
            }
        }

        segment_info->bins = newval;
        histogram_reset(true, false); /* already locked => true, don't remove */
        LWLockRelease(segment_info->lock);
    }

    HOOK_RETURN(true);

}

#if (PG_VERSION_NUM >= 90100)
static void set_histogram_bins_width_hook(int newval, void *extra) {
#else
static bool set_histogram_bins_width_hook(int newval, bool doit, GucSource source) {
#endif

    if (! histogram_is_dynamic) {
        elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
                      "it's not possible to change the bin width.");

        HOOK_RETURN(false);
    }

    if (segment_info) {
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);

        segment_info->step = newval;

        /* if the histogram is logarithmic, there really is not much point
         * in sending more than 32 bins (or something like that) */
        if (segment_info->type == HISTOGRAM_LOG) {
            int max_count = (int)ceil(log2(INT_MAX/segment_info->step));
            if (segment_info->bins > max_count) {
                elog(NOTICE, "the max bin count %d is too high for log histogram with "
                "%d ms resolution, using %d", segment_info->bins, segment_info->step, max_count);
                segment_info->bins = max_count;
            }
        }

        histogram_reset(true, false); /* already locked => true, don't remove */
        LWLockRelease(segment_info->lock);
    }

    HOOK_RETURN(true);

}

#if (PG_VERSION_NUM >= 90100)
static void set_histogram_sample_hook(int newval, void *extra) {
#else
static bool set_histogram_sample_hook(int newval, bool doit, GucSource source) {
#endif

    if (! histogram_is_dynamic ) {
        elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
                      "it's not possible to change the sampling rate.");

        HOOK_RETURN(false);
    }

    if (segment_info) {
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);
        segment_info->sample_pct = newval;
        histogram_reset(true, false); /* already locked => true, don't remove */
        LWLockRelease(segment_info->lock);
    }

    HOOK_RETURN(true);

}


#if (PG_VERSION_NUM >= 90100)
static void set_max_databases_hook(int newval, void *extra) {
#else
static bool set_max_databases_hook(int newval, bool doit, GucSource source) {
#endif

    if (! histogram_is_dynamic ) {
        elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
                      "it's not possible to change the number of databases.");

        HOOK_RETURN(false);
    }

    if (segment_info) {
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);
        segment_info->max_databases = newval;
        histogram_reset(true, false); /* already locked => true, don't remove */
        LWLockRelease(segment_info->lock);
    }

    HOOK_RETURN(true);

}


#if (PG_VERSION_NUM >= 90100)
static void set_histogram_type_hook(int newval, void *extra) {
#else
static bool set_histogram_type_hook(int newval, bool doit, GucSource source) {
#endif

    if (! histogram_is_dynamic) {
        elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
                      "it's not possible to change the histogram type.");

        HOOK_RETURN(false);
    }

    if (segment_info) {
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);

        segment_info->type = newval;

        /* if the histogram is logarithmic, there really is not much point
         * in sending more than 32 bins (or something like that) */
        if (segment_info->type == HISTOGRAM_LOG) {
            int max_count = (int)ceil(log2(INT_MAX/segment_info->step));
            if (segment_info->bins > max_count) {
                elog(NOTICE, "the max bin count %d is too high for log histogram with "
                "%d ms resolution, using %d", segment_info->bins, segment_info->step, max_count);
                segment_info->bins = max_count;
            }
        }

        histogram_reset(true, false); /* already locked => true, don't remove */
        LWLockRelease(segment_info->lock);
    }

    HOOK_RETURN(true);

}


#if (PG_VERSION_NUM >= 90100)
static void set_histogram_track_utility(bool newval, void *extra) {
#else
static bool set_histogram_track_utility(bool newval, bool doit, GucSource source) {
#endif

    if (! histogram_is_dynamic) {
        elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
                      "it's not possible to change the histogram type.");

        HOOK_RETURN(false);
    }

    if (segment_info) {
        LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);
        segment_info->track_utility = newval;
        histogram_reset(true, false); /* already locked => true, don't remove */
        LWLockRelease(segment_info->lock);
    }

    HOOK_RETURN(true);

}

/* The histogram is enabled when the number of bins is positive or when
 * the histogram is dynamic (in that case we can't rely on the bins number
 * as it may change next second). */
static
bool query_histogram_enabled() {

    /* when the histogram is static, check the number of bins (does not
     * make much sense, I guess - it's probably better to remove the
     * library from the config altogether than just setting 0). */
    if (! default_histogram_dynamic)
        return (default_histogram_bins > 0);

    return true;

}

/* Amount of memory required to store all the data. */
size_t histogram_segment_size(int max_databases) {

    return sizeof(segment_info_t)                   /* segment structure */
            + (max_databases * sizeof(db_info_t))   /* list of databases (at most max_databases) */
            + ((max_databases + 1) * sizeof(histogram_info_t)); /* histograms (databases + global) */

}

/* Number of LWLocks for segment and per-database histograms (and the global one) */
size_t histogram_segment_locks(int max_databases) {

    /* one lock per segment, one for global histogram, one for each database histogram */
    return (2 + max_databases);

}
