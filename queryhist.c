#include <stdio.h>
#include <stdlib.h>
#include <sys/shm.h>
#include <sys/stat.h>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>

#include "postgres.h"
#include "queryhist.h"

#include "commands/explain.h"
#include "executor/instrument.h"
#include "utils/guc.h"

static const struct config_enum_entry histogram_type_options[] = {
    {"linear", HISTOGRAM_LINEAR, false},
    {"log", HISTOGRAM_LOG, false},
    {NULL, 0, false}
};

static int nesting_level = 0;

typedef unsigned long hist_bin_count_t;
typedef float4        hist_bin_time_t;

/* The histogram itself is stored in a shared memory segment
 * with this layout
 * 
 * - users (int => 4B)
 * - bins (int => 4B)
 * - step (int => 4B)
 * - type (int => 4B)
 * - sample (int => 4B)
 * - count bins (HIST_BINS_MAX+1) x sizeof(unsigned long)
 * - time  bins (HIST_BINS_MAX+1) x sizeof(unsigned long)
 * 
 * This segment is initialized in the first process that accesses it (see
 * query_hist_init function).
 */
#define SEGMENT_KEY   43873
#define SEMAPHORE_KEY 43874

#define HIST_BINS_MAX 1000

static bool  histogram_initialized = FALSE;
static int * histogram_users;
static int * histogram_bins;
static int * histogram_step;
static histogram_type_t * histogram_type;
static int * histogram_sample_pct;

/* default values */
static int default_histogram_bins = 100;
static int default_histogram_step = 100;
static int default_histogram_sample_pct = 5;
static int default_histogram_type = HISTOGRAM_LINEAR;

static hist_bin_count_t * histogram_count_bins;
static hist_bin_time_t * histogram_time_bins;

/* semaphore used to sync access to the shared segment */
static int semaphore_id;

#define query_histogram_enabled() \
    ((histogram_initialized) && (nesting_level == 0) && ((*histogram_bins) > 0))

/* Saved hook values in case of unload */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static void set_histogram_bins_count_hook(int newval, void *extra);
static void set_histogram_bins_width_hook(int newval, void *extra);
static void set_histogram_sample_hook(int newval, void *extra);
static void set_histogram_type_hook(int newval, void *extra);

void        _PG_init(void);
void        _PG_fini(void);

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
                    ScanDirection direction,
                    long count);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);

/* private functions */
void query_hist_init();
void query_hist_add_query(hist_bin_time_t duration);

void semaphore_lock();
void semaphore_unlock();

/*
 * Module load callback
 */
void
_PG_init(void)
{
    /* Define custom GUC variables. */
    DefineCustomIntVariable("query_histogram.bin_count",
                         "Sets the number of bins of the histogram.",
                         "Zero disables collecting the histogram.",
                            &default_histogram_bins,
                            100,
                            0, 1000,
                            PGC_SUSET,
                            GUC_UNIT_MS,
                            NULL,
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
                            NULL,
                            &set_histogram_bins_width_hook,
                            NULL);
    
    DefineCustomIntVariable("query_histogram.sample_pct",
                         "What portion of the queries should be sampled (in percent).",
                            NULL,
                            &default_histogram_sample_pct,
                            5,
                            1, 100,
                            PGC_SUSET,
                            GUC_UNIT_MS,
                            NULL,
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
                             NULL,
                             &set_histogram_type_hook,
                             NULL);

    EmitWarningsOnPlaceholders("query_histogram");

    /* Install hooks. */
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = explain_ExecutorStart;
    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = explain_ExecutorRun;
    prev_ExecutorFinish = ExecutorFinish_hook;
    ExecutorFinish_hook = explain_ExecutorFinish;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = explain_ExecutorEnd;
    
    /* FIXME reference the proper GUC default values somehow */
    query_hist_init();
    
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
    ExecutorFinish_hook = prev_ExecutorFinish;
    ExecutorEnd_hook = prev_ExecutorEnd;
    
    /* FIXME I can't mark the segment as "deletable" as it would cause
     * occasional data loss (the OS could remove the histogram segment).
     * Anyway it's just 8kB of data, so it's not a big issue.
     */
    if (histogram_initialized) {
        --(*histogram_users);
    }
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

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
    
    if (queryDesc->totaltime && query_histogram_enabled())
    {
        float seconds;
        
        /*
         * Make sure stats accumulation is done.  (Note: it's okay if several
         * levels of hook all do this.)
         */
        InstrEndLoop(queryDesc->totaltime);
        
        /* Log plan if duration is exceeded. */
        seconds = queryDesc->totaltime->total;

        /* N % 100 returns value 0-99, so we need to subtract 1 from the sample_pct */
        if ((histogram_initialized) && (rand() % 100 < (default_histogram_sample_pct-1))) {
            query_hist_add_query(seconds);
        }
        
    }

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
    
}

/* FIXME There should be a semaphore / mutex guarding the segment. */
void query_hist_init() {

    int segment_id;
    int required_size;
    char * data;
    bool created = FALSE;
    
    /* initialize semaphore */
    semaphore_id = semget(SEMAPHORE_KEY, 1, IPC_CREAT);
    
    if (semaphore_id == -1) {
        /* not sure if this is enough - maybe it will crash later or something */
        elog(ERROR, "semaphore for histogram synchronization not created");
        return;
    }
    
    /* initialize shared memory segment */
    required_size = 5*sizeof(int) + (HIST_BINS_MAX+1) * sizeof(hist_bin_count_t) + (HIST_BINS_MAX+1) * sizeof(hist_bin_time_t);
    
    elog(DEBUG1, "initializing histogram segment (size: %d B)", required_size);
    
    /* lock the semaphore */
    semaphore_lock();
    
    /* check if there's an existing segment */
    segment_id = shmget(SEGMENT_KEY, required_size, S_IRUSR | S_IWUSR);
    if (segment_id == -1) {
        
        /* try to create a new segment */
        segment_id = shmget(SEGMENT_KEY, required_size, IPC_CREAT | S_IRUSR | S_IWUSR);
        if (segment_id == -1) {
            /* not sure if this is enough - maybe it will crash later or something */
            elog(ERROR, "shared memory segment (histogram) not allocated");
            return;
        }
        
        elog(DEBUG1, "shared memory segment (histogram) %d successfully created", SEGMENT_KEY);
        created = TRUE;
        
    }
    
    /* there should be a segment existing, try to attach it */
    data = (char*)shmat(segment_id, 0, 0);
    
    if (data == (void*)-1) {
        elog(ERROR, "shared memory segment (histogram) %d not attached", SEGMENT_KEY);
        return;
    }
    
    elog(DEBUG1, "shared memory segment (histogram) successfully attached");
    
    /* set the users/bins/step/sample properly */
    histogram_users = (int*)data;
    histogram_bins  = (int*)(data + sizeof(int));
    histogram_step  = (int*)(data + 2*sizeof(int));
    histogram_type  = (histogram_type_t*)(data + 3*sizeof(int));
    histogram_sample_pct = (int*)(data + 3*sizeof(int) + sizeof(histogram_type_t));
    
    histogram_count_bins = (hist_bin_count_t*)(data + 4*sizeof(int) + sizeof(histogram_type_t));
    histogram_time_bins  =  (hist_bin_time_t*)(data + 4*sizeof(int) + sizeof(histogram_type_t) + (HIST_BINS_MAX+1)*sizeof(hist_bin_count_t));
    
    /* increase the histogram users count */
    ++(*histogram_users);

    /* reset the histogram (set to zeroes), */
    if (created) {
        query_hist_reset(true);
    } else {
        default_histogram_bins = (*histogram_bins);
        default_histogram_step = (*histogram_step);
        default_histogram_sample_pct = (*histogram_sample_pct);
    }
    
    /* increase the number or users */
    ++(*histogram_users);
    
    /* init done, unlock the semaphore */
    semaphore_unlock();
    
    /* initialized successfully */
    histogram_initialized = TRUE;

    /* seed the random generator */
    srand(SEGMENT_KEY);
    
}

void query_hist_reset(bool locked) {
    
    if (histogram_initialized) {
        
        if (! locked) { semaphore_lock(); }
    
        (*histogram_bins) = default_histogram_bins;
        (*histogram_step) = default_histogram_step;
        (*histogram_sample_pct) = default_histogram_sample_pct;
        (*histogram_type) = default_histogram_type;
        
        memset(histogram_count_bins, 0, (HIST_BINS_MAX+1)*sizeof(hist_bin_count_t));
        memset(histogram_time_bins,  0, (HIST_BINS_MAX+1)*sizeof(hist_bin_time_t));
        
        if (! locked) { semaphore_unlock(); }
        
    }
    
}

void query_hist_refresh() {
    
    if (histogram_initialized) {
        
        semaphore_lock();
    
        default_histogram_bins = (*histogram_bins);
        default_histogram_step = (*histogram_step);
        default_histogram_sample_pct = (*histogram_sample_pct);
        default_histogram_type = (*histogram_type);
        
        semaphore_unlock();
        
    }
    
}

void query_hist_add_query(hist_bin_time_t duration) {
    
    int bin;
    
    semaphore_lock();
    
    bin = (int)ceil(duration * 1000.0) / (*histogram_step);
    
    /* queries that take longer than the last bin should go to
     * the (HIST_BINS_MAX+1) bin */
    if (bin >= (*histogram_bins)) {
        bin = (*histogram_bins);
    }
    
    histogram_count_bins[bin] += 1;
    histogram_time_bins[bin] += duration;
    
    semaphore_unlock();
    
}

histogram_data * query_hist_get_data() {
    
    int i = 0;
    
    histogram_data * tmp = (histogram_data *)palloc(sizeof(histogram_data));
    
    memset(tmp, 0, sizeof(histogram_data));
    
    semaphore_lock();

    tmp->bins_count = (*histogram_bins);
    tmp->bins_width = (*histogram_step);
    
    if ((*histogram_bins) > 0) {
    
        tmp->count_data = (hist_bin_count_t *) palloc(sizeof(hist_bin_count_t) * ((*histogram_bins)+1));
        tmp->time_data  =  (hist_bin_time_t *) palloc(sizeof(hist_bin_time_t)  * ((*histogram_bins)+1));
        
        memcpy(tmp->count_data, histogram_count_bins, sizeof(hist_bin_count_t) * ((*histogram_bins)+1));
        memcpy(tmp->time_data,  histogram_time_bins,  sizeof(hist_bin_time_t)  * ((*histogram_bins)+1));
        
        for (i = 0; i < (*histogram_bins)+1; i++) {
            tmp->total_count += tmp->count_data[i];
            tmp->total_time  += tmp->time_data[i];
        }
        
    }
    
    semaphore_unlock();

    return tmp;

}

static
void set_histogram_bins_count_hook(int newval, void *extra) {
    
    default_histogram_bins = newval;

    if (histogram_initialized) {
        semaphore_lock();
        default_histogram_step = (*histogram_step);
        default_histogram_sample_pct = (*histogram_sample_pct);
        default_histogram_type = (*histogram_type);
        semaphore_unlock();
    }
    
    query_hist_reset(false);
    
}

static
void set_histogram_bins_width_hook(int newval, void *extra) {
    default_histogram_step = newval;
    
    if (histogram_initialized) {
        semaphore_lock();
        default_histogram_bins = (*histogram_bins);
        default_histogram_sample_pct = (*histogram_sample_pct);
        default_histogram_type = (*histogram_type);
        semaphore_unlock();
    }
    
    query_hist_reset(false);
}

static
void set_histogram_sample_hook(int newval, void *extra) {
    
    default_histogram_sample_pct = newval;
    
    if (histogram_initialized) {
        semaphore_lock();
        default_histogram_bins = (*histogram_bins);
        default_histogram_step = (*histogram_step);
        default_histogram_type = (*histogram_type);
        semaphore_unlock();
    }

    query_hist_reset(false);
}

static
void set_histogram_type_hook(int newval, void *extra) {
    
    default_histogram_type = newval;
    
    if (histogram_initialized) {
        semaphore_lock();
        default_histogram_bins = (*histogram_bins);
        default_histogram_step = (*histogram_step);
        default_histogram_sample_pct = (*histogram_sample_pct);
        semaphore_unlock();
    }
    
    query_hist_reset(false);
}

void semaphore_lock() {
    
    struct sembuf operations[1];
    
    operations[0].sem_num = 0;
    operations[0].sem_op = -1;
    operations[0].sem_flg = SEM_UNDO;
    
    semop (semaphore_id, operations, 1);
    
}

void semaphore_unlock() {
    
    struct sembuf operations[1];

    operations[0].sem_num = 0;
    operations[0].sem_op = 1;
    operations[0].sem_flg = SEM_UNDO;

    semop (semaphore_id, operations, 1);

}
