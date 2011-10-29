#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "queryhist.h"

#include "commands/explain.h"
#include "executor/instrument.h"
#include "utils/guc.h"

/* How are the histogram bins scaled? */
typedef enum {
    HISTOGRAM_LINEAR,
    HISTOGRAM_LOG
} histogram_type;

static const struct config_enum_entry histogram_type_options[] = {
    {"linear", HISTOGRAM_LINEAR, false},
    {"log", HISTOGRAM_LOG, false}
};

#define HIST_BINS_MAX 1000

static int query_histogram_bins_count = 0; /* number of bins (0 - disable) */
static int query_histogram_bins_width = 10; /* msec (bin width) */
static int query_histogram_sample_pct = 5; /* portion of queries to sample */
static int query_histogram_type = HISTOGRAM_LINEAR; /* histogram type */

static int nesting_level = 0;

static unsigned long histogram_count_bins[HIST_BINS_MAX+1];
static unsigned long histogram_time_bins[HIST_BINS_MAX+1];

#define query_histogram_enabled() \
    ((query_histogram_bins_count > 0) && (nesting_level == 0))

/* Saved hook values in case of unload */
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;

static void set_histogram_bins_count_hook(int newval, void *extra);
static void set_histogram_bins_width_hook(int newval, void *extra);
static void set_histogram_sample_hook(int newval, void *extra);

void            _PG_init(void);
void            _PG_fini(void);

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);

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
                            &query_histogram_bins_count,
                            0,
                            0, 1000,
                            PGC_SUSET,
                            GUC_UNIT_MS,
                            NULL,
                            &set_histogram_bins_count_hook,
                            NULL);
    
    DefineCustomIntVariable("query_histogram.bin_width",
                         "Sets the width of the histogram bin.",
                            NULL,
                            &query_histogram_bins_width,
                            10,
                            1, 1000,
                            PGC_SUSET,
                            GUC_UNIT_MS,
                            NULL,
                            &set_histogram_bins_width_hook,
                            NULL);
    
    DefineCustomIntVariable("query_histogram.sample_pct",
                         "What portion of the queries should be sampled (in percent).",
                            NULL,
                            &query_histogram_sample_pct,
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
                             &query_histogram_type,
                             HISTOGRAM_LINEAR,
                             histogram_type_options,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
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
    
    query_hist_init(query_histogram_bins_count, query_histogram_bins_width);
    
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
        int msec;

        /*
         * Make sure stats accumulation is done.  (Note: it's okay if several
         * levels of hook all do this.)
         */
        InstrEndLoop(queryDesc->totaltime);

        /* Log plan if duration is exceeded. */
        msec = (int)round(queryDesc->totaltime->total * 1000.0);
        
        if (rand() % 100 < query_histogram_sample_pct) {
            query_hist_add_query((int)round(msec));
        }
        
    }

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

void query_hist_free() {
    
    /* free the original histogram (if needed) */
    if (histogram_count_bins != NULL) {
        pfree(histogram_count_bins);
        pfree(histogram_time_bins);
    }

}

void query_hist_init(int bins, int step) {

    srand(bins + step);
    
    query_hist_reset(bins);
    
}

void query_hist_reset(int bins) {
    
    memset(histogram_count_bins, 0, HIST_BINS_MAX+1);
    memset(histogram_time_bins,  0, HIST_BINS_MAX+1);
}

void query_hist_add_query(int duration) {
    
    int bin = duration / query_histogram_bins_width;
    
    /* queries that take longer than the last bin should go to
     * the (HIST_BINS_MAX+1) bin */
    if (bin >= HIST_BINS_MAX) {
        bin = HIST_BINS_MAX;
    }
    
    histogram_count_bins[bin] += 1;
    histogram_time_bins[bin] += duration;
    
}

histogram_data * query_hist_get_data() {
    
    int i = 0;
    
    histogram_data * tmp = (histogram_data *)palloc(sizeof(histogram_data));
    
    memset(tmp, 0, sizeof(histogram_data));

    tmp->bins_count = query_histogram_bins_count;
    tmp->bins_width = query_histogram_bins_width;
    
    if (query_histogram_bins_count > 0) {
    
        tmp->count_data = (unsigned long *) palloc(sizeof(unsigned long) * (query_histogram_bins_count+1));
        tmp->time_data  = (unsigned long *) palloc(sizeof(unsigned long) * (query_histogram_bins_count+1));
        
        memcpy(tmp->count_data, histogram_count_bins, sizeof(unsigned long) * (query_histogram_bins_count+1));
        memcpy(tmp->time_data,  histogram_time_bins,  sizeof(unsigned long) * (query_histogram_bins_count+1));
        
        for (i = 0; i < query_histogram_bins_count+1; i++) {
            tmp->total_count += tmp->count_data[i];
            tmp->total_time  += tmp->time_data[i];
        }
        
    }

    return tmp;

}

int  query_hist_get_bins() {
    return query_histogram_bins_count;
}

int  query_hist_get_step() {
    return query_histogram_bins_width;
}

static
void set_histogram_bins_count_hook(int newval, void *extra) {
    query_hist_init(newval, query_histogram_bins_width);
}

static
void set_histogram_bins_width_hook(int newval, void *extra) {
    query_hist_init(query_histogram_bins_count, newval);
}

static
void set_histogram_sample_hook(int newval, void *extra) {
    query_hist_init(query_histogram_bins_count, query_histogram_bins_width);
}