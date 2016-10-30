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
#include "storage/shmem.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "utils/guc.h"
#include "tcop/utility.h"

#include "common/md5.h"

#include "queryhist.h"

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

static void set_histogram_bins_count_hook(int newval, void *extra);
static void set_histogram_bins_width_hook(int newval, void *extra);
static void set_histogram_sample_hook(int newval, void *extra);
static void set_histogram_type_hook(int newval, void *extra);
static void set_histogram_track_utility(bool newval, void *extra);

static const char * show_histogram_bins_count_hook(void);
static const char * show_histogram_bins_width_hook(void);
static const char * show_histogram_sample_hook(void);
static const char * show_histogram_type_hook(void);
static const char * show_histogram_track_utility(void);

/* return from a hook */
#define HOOK_RETURN(a)	return;

static void query_hist_add_query(time_bin_t duration);
static bool query_histogram_enabled(void);
static int get_hist_bin(int bins, int step, time_bin_t duration);

static size_t get_histogram_size(void);

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
#define SEGMENT_NAME	"query_histogram"

/* default values (used for init) */
static bool default_histogram_dynamic = false;
static bool default_histogram_utility = true; /* track DDL */
static int  default_histogram_bins = 100;
static int  default_histogram_step = 100;
static int  default_histogram_sample_pct = 5;
static int  default_histogram_type = HISTOGRAM_LINEAR;

/* set at the end of init */
static bool histogram_is_dynamic = true;

/* TODO It might be useful to allow 'per database' histograms, or to collect
 *	  the data only for some of the databases. So there might be options
 *
 *		query_histogram.per_database={true|false}
 *		query_histogram.databases= ... list of database names
 *
 *	  and the per_database would require databases (so that we know how
 *	  much memory to allocate etc.)
 */

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

void		_PG_init(void);
void		_PG_fini(void);

static void histogram_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void histogram_ExecutorRun(QueryDesc *queryDesc,
					ScanDirection direction,
					uint64 count);
static void histogram_ExecutorEnd(QueryDesc *queryDesc);

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

static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static void histogram_ExecutorFinish(QueryDesc *queryDesc);

/* the whole histogram (info and data) */
static histogram_info_t * shared_histogram_info = NULL;

/*
 * Module load callback
 */
void
_PG_init(void)
{

	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the query_histogram functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Define custom GUC variables. */
	DefineCustomBoolVariable("query_histogram.dynamic",
							  "Dynamic histogram may be modified on the fly.",
							 NULL,
							 &default_histogram_dynamic,
							 false,
							 PGC_BACKEND,
							 0,
							 NULL,
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
							 NULL,
							 &set_histogram_track_utility,
							 &show_histogram_track_utility);

	DefineCustomIntVariable("query_histogram.bin_count",
						 "Sets the number of bins of the histogram.",
						 "Zero disables collecting the histogram.",
							&default_histogram_bins,
							100,
							0, 1000,
							PGC_SUSET,
							0,
							NULL,
							&set_histogram_bins_count_hook,
							&show_histogram_bins_count_hook);

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
							&show_histogram_bins_width_hook);

	DefineCustomIntVariable("query_histogram.sample_pct",
						 "What portion of the queries should be sampled (in percent).",
							NULL,
							&default_histogram_sample_pct,
							5,
							1, 100,
							PGC_SUSET,
							0,
							NULL,
							&set_histogram_sample_hook,
							&show_histogram_sample_hook);

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
							 &show_histogram_type_hook);

	EmitWarningsOnPlaceholders("query_histogram");

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in histogram_shmem_startup().
	 */
	RequestAddinShmemSpace(get_histogram_size());
	RequestNamedLWLockTranche("query_histogram", 1);

	/* Install hooks. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = histogram_shmem_startup;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = histogram_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = histogram_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = histogram_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = histogram_ExecutorEnd;
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
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	shmem_startup_hook = prev_shmem_startup_hook;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
histogram_ExecutorStart(QueryDesc *queryDesc, int eflags)
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
histogram_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
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
histogram_ExecutorFinish(QueryDesc *queryDesc)
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
histogram_ExecutorEnd(QueryDesc *queryDesc)
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
				LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);
				query_hist_add_query(seconds);
				LWLockRelease(shared_histogram_info->lock);
			}

		} else {
			/* when the histogram is dynamic, we have to lock it first, as we
			 * will access the sample_pct in the histogram */
			LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
			if ((shared_histogram_info->bins > 0) && (rand() % 100 <  shared_histogram_info->sample_pct)) {
				LWLockRelease(shared_histogram_info->lock);
				LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);
				query_hist_add_query(seconds);
			}
			LWLockRelease(shared_histogram_info->lock);

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
		float	   seconds;

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
				LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);
				query_hist_add_query(seconds);
				LWLockRelease(shared_histogram_info->lock);
			}

		} else {
			/* when the histogram is dynamic, we have to lock it first, as we
			 * will access the sample_pct in the histogram */
			LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
			if ((shared_histogram_info->bins > 0) && (rand() % 100 <  shared_histogram_info->sample_pct)) {
				LWLockRelease(shared_histogram_info->lock);
				LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);
				query_hist_add_query(seconds);
			}
			LWLockRelease(shared_histogram_info->lock);

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
static void
histogram_shmem_startup()
{
	bool found = FALSE;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	shared_histogram_info = ShmemInitStruct(SEGMENT_NAME,
					sizeof(histogram_info_t),
					&found);

	elog(DEBUG1, "initializing query histogram segment (size: %lu B)", sizeof(histogram_info_t));

	if (! found) {

		/* First time through ... */
		shared_histogram_info->lock = &(GetNamedLWLockTranche("query_histogram"))->lock;

		shared_histogram_info->type = default_histogram_type;
		shared_histogram_info->bins = default_histogram_bins;
		shared_histogram_info->step = default_histogram_step;
		shared_histogram_info->sample_pct = default_histogram_sample_pct;
		shared_histogram_info->last_reset = GetCurrentTimestamp();

		memset(shared_histogram_info->count_bins, 0, (HIST_BINS_MAX+1)*sizeof(count_bin_t));
		memset(shared_histogram_info->time_bins,  0, (HIST_BINS_MAX+1)*sizeof(time_bin_t));

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
	// srand((int)shared_histogram_info);
}

/* Loads the histogram data from a file (and checks that the md5 hash of the contents matches). */
static void histogram_load_from_file(void)
{
	FILE * file;
	char hash_file[16];
	char hash_comp[16];
	char * buffer = NULL;

	/* load the histogram from the file */
	file = AllocateFile(HISTOGRAM_DUMP_FILE, PG_BINARY_R);

	if ((file == NULL) && (errno == ENOENT)) {
		elog(DEBUG1, "file with the histogram does not exist");
		return;
	} else if (file == NULL) {
		goto error;
	}

	/* read the first 16 bytes (should be a MD5 hash of the segment) */
	if (fread(hash_file, 16, 1, file) != 1)
		goto error;

	/* read the histogram (into buffer) */
	buffer = palloc(sizeof(histogram_info_t));
	if (fread(buffer, sizeof(histogram_info_t), 1, file) != 1)
		goto error;

	/* compute md5 hash of the buffer */
	pg_md5_binary(buffer, sizeof(histogram_info_t), hash_comp);

	/* check that the hashes are equal (the file is not corrupted) */
	if (memcmp(hash_file, hash_comp, 16) == 0) {

		/* now we know the buffer contains 'valid' histogram */

		/* we can copy it into the shared segment iff the histogram
		 * is static and has the same parameters, or if it's dynamic
		 * (in this case the parameters may be arbitrary) */
		if ((default_histogram_dynamic) ||
			((! default_histogram_dynamic) && (((histogram_info_t*)buffer)->bins == default_histogram_bins)
										   && (((histogram_info_t*)buffer)->step == default_histogram_step)
										   && (((histogram_info_t*)buffer)->sample_pct == default_histogram_sample_pct)
										   && (((histogram_info_t*)buffer)->type == default_histogram_type))) {

			memcpy(shared_histogram_info, buffer, sizeof(histogram_info_t));

			/* FIXME Is this necessary? */
			shared_histogram_info->lock = &(GetNamedLWLockTranche("query_histogram"))->lock;

			/* copy the values from the histogram */
			default_histogram_type = shared_histogram_info->type;
			default_histogram_bins = shared_histogram_info->bins;
			default_histogram_step = shared_histogram_info->step;
			default_histogram_sample_pct = shared_histogram_info->sample_pct;

			elog(DEBUG1, "successfully loaded query histogram from a file : %s",
				HISTOGRAM_DUMP_FILE);

		} else {

			elog(WARNING, "can't load the histogram from %s because the parameters differ",
				 HISTOGRAM_DUMP_FILE);

		}

	} else {
		elog(WARNING, "can't load the histogram from %s because the hash is incorrect",
			 HISTOGRAM_DUMP_FILE);
	}

	FreeFile(file);
	pfree(buffer);

	return;

error: /* error handling */
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read query_histogram file \"%s\": %m",
					HISTOGRAM_DUMP_FILE)));
	if (buffer)
		pfree(buffer);
	if (file)
		FreeFile(file);
}

/* Dumps the histogram data into a file (with a md5 hash of the contents at the beginning). */
static void
histogram_shmem_shutdown(int code, Datum arg)
{
	FILE * file;
	char buffer[16];

	file = AllocateFile(HISTOGRAM_DUMP_FILE, PG_BINARY_W);
	if (file == NULL)
		goto error;

	/* lets compute MD5 hash of the shared memory segment and write it to
	 * the beginning of the file */
	pg_md5_binary(shared_histogram_info, sizeof(histogram_info_t), buffer);

	if (fwrite(buffer, 16, 1, file) != 1)
		goto error;

	/* now write the actual shared segment */
	if (fwrite(shared_histogram_info, sizeof(histogram_info_t), 1, file) != 1)
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

/* need an exclusive lock to modify the histogram */
void
query_hist_reset(bool locked)
{
	if (! shared_histogram_info) {
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("query_histogram must be loaded via shared_preload_libraries")));
	}

	if (! locked) {
		LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);
	}

	memset(shared_histogram_info->count_bins, 0, (HIST_BINS_MAX+1)*sizeof(count_bin_t));
	memset(shared_histogram_info->time_bins,  0, (HIST_BINS_MAX+1)*sizeof(time_bin_t));

	shared_histogram_info->last_reset = GetCurrentTimestamp();

	/* if it was not locked before, we can release the lock now */
	if (! locked) {
		LWLockRelease(shared_histogram_info->lock);
	}
}

/* needs to be already locked */
static void
query_hist_add_query(time_bin_t duration)
{
	int bin = get_hist_bin(shared_histogram_info->bins, shared_histogram_info->step, duration);

	shared_histogram_info->count_bins[bin] += 1;
	shared_histogram_info->time_bins[bin] += duration;
}

static int
get_hist_bin(int bins, int step, time_bin_t duration)
{
	int bin = 0;

	if (shared_histogram_info->type == HISTOGRAM_LINEAR) {
		bin = (int)floor((duration * 1000.0) / (shared_histogram_info->step));
	} else {
		bin = (int)floor(log2(1 + ((duration * 1000.0) / (shared_histogram_info->step))));
	}

	/* queries that take longer than the last bin should go to
	 * the (HIST_BINS_MAX+1) bin */
	return (bin >= (shared_histogram_info->bins)) ? (shared_histogram_info->bins) : bin;
}

TimestampTz
get_hist_last_reset()
{
	TimestampTz  timestamp;

	if (! shared_histogram_info) {
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("query_histogram must be loaded via shared_preload_libraries")));
	}

	LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
	timestamp = shared_histogram_info->last_reset;
	LWLockRelease(shared_histogram_info->lock);

	return timestamp;
}

histogram_data *
query_hist_get_data(bool scale)
{
	int i = 0;
	double coeff = 0;
	histogram_data * tmp = NULL;

	if (! shared_histogram_info) {
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("query_histogram must be loaded via shared_preload_libraries")));
	}

	tmp = (histogram_data *)palloc(sizeof(histogram_data));

	memset(tmp, 0, sizeof(histogram_data));

	/* we can do this using a shared lock */
	LWLockAcquire(shared_histogram_info->lock, LW_SHARED);

	tmp->histogram_type = (shared_histogram_info->type);
	tmp->bins_count = (shared_histogram_info->bins);
	tmp->bins_width = (shared_histogram_info->step);

	if (shared_histogram_info->bins > 0) {

		tmp->count_data = (count_bin_t *) palloc(sizeof(count_bin_t) * (shared_histogram_info->bins+1));
		tmp->time_data  =  (time_bin_t *) palloc(sizeof(time_bin_t)  * (shared_histogram_info->bins+1));

		memcpy(tmp->count_data, shared_histogram_info->count_bins, sizeof(count_bin_t) * (shared_histogram_info->bins+1));
		memcpy(tmp->time_data,  shared_histogram_info->time_bins,  sizeof(time_bin_t)  * (shared_histogram_info->bins+1));

		/* check if we need to scale the histogram */
		if (scale && (shared_histogram_info->sample_pct < 100)) {
			coeff = (100.0 / (shared_histogram_info->sample_pct));
			for (i = 0; i < (shared_histogram_info->bins+1); i++) {
				tmp->count_data[i] = tmp->count_data[i] * coeff;
				tmp->time_data[i]  = tmp->time_data[i] * coeff;
			}
		}

		for (i = 0; i < (shared_histogram_info->bins+1); i++) {
			tmp->total_count += tmp->count_data[i];
			tmp->total_time  += tmp->time_data[i];
		}

	}

	/* release the lock */
	LWLockRelease(shared_histogram_info->lock);

	return tmp;
}

static void
set_histogram_bins_count_hook(int newval, void *extra)
{
	if (! histogram_is_dynamic) {
		elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
					  "it's not possible to change the number of bins.");

		HOOK_RETURN(false);
	}

	if (shared_histogram_info) {
		LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);

		/* if the histogram is logarithmic, there really is not much point
		 * in sending more than 32 bins (or something like that) */
		if (shared_histogram_info->type == HISTOGRAM_LOG) {
			int max_count = (int)ceil(log2(INT_MAX/shared_histogram_info->step));
			if (newval > max_count) {
				elog(NOTICE, "the max bin count %d is too high for log histogram with "
				"%d ms resolution, using %d", newval, shared_histogram_info->step, max_count);
				newval = max_count;
			}
		}

		shared_histogram_info->bins = newval;
		query_hist_reset(true);
		LWLockRelease(shared_histogram_info->lock);
	}

	HOOK_RETURN(true);
}

static const char *
show_histogram_bins_count_hook(void)
{
	static char nbuf[4];

	int bins_count = default_histogram_bins;

	/* if the histogram is dynamic and was initialized, get value from it */
	if (histogram_is_dynamic && shared_histogram_info)
	{
		LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
		bins_count = shared_histogram_info->bins;
		LWLockRelease(shared_histogram_info->lock);
	}

	snprintf(nbuf, sizeof(nbuf), "%d", bins_count);

	return nbuf;
}

static void
set_histogram_bins_width_hook(int newval, void *extra)
{
	if (! histogram_is_dynamic) {
		elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
					  "it's not possible to change the bin width.");

		HOOK_RETURN(false);
	}

	if (shared_histogram_info) {
		LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);

		shared_histogram_info->step = newval;

		/* if the histogram is logarithmic, there really is not much point
		 * in sending more than 32 bins (or something like that) */
		if (shared_histogram_info->type == HISTOGRAM_LOG) {
			int max_count = (int)ceil(log2(INT_MAX/shared_histogram_info->step));
			if (shared_histogram_info->bins > max_count) {
				elog(NOTICE, "the max bin count %d is too high for log histogram with "
				"%d ms resolution, using %d", shared_histogram_info->bins, shared_histogram_info->step, max_count);
				shared_histogram_info->bins = max_count;
			}
		}

		query_hist_reset(true);
		LWLockRelease(shared_histogram_info->lock);
	}

	HOOK_RETURN(true);
}

static const char *
show_histogram_bins_width_hook(void)
{
	static char nbuf[4];
	int step = default_histogram_step;

	/* if the histogram is dynamic and was initialized, get value from it */
	if (histogram_is_dynamic && shared_histogram_info)
	{
		LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
		step = shared_histogram_info->step;
		LWLockRelease(shared_histogram_info->lock);
	}

	snprintf(nbuf, sizeof(nbuf), "%d", step);

	return nbuf;
}


static void
set_histogram_sample_hook(int newval, void *extra)
{

	if (! histogram_is_dynamic ) {
		elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
					  "it's not possible to change the sampling rate.");

		HOOK_RETURN(false);
	}

	if (shared_histogram_info) {
		LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);
		shared_histogram_info->sample_pct = newval;
		query_hist_reset(true);
		LWLockRelease(shared_histogram_info->lock);
	}

	HOOK_RETURN(true);
}

static const char *
show_histogram_sample_hook(void)
{
	static char nbuf[4];

	int sample_pct = default_histogram_sample_pct;

	/* if the histogram is dynamic and was initialized, get value from it */
	if (histogram_is_dynamic && shared_histogram_info)
	{
		LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
		sample_pct = shared_histogram_info->sample_pct;
		LWLockRelease(shared_histogram_info->lock);
	}

	snprintf(nbuf, sizeof(nbuf), "%d", sample_pct);

	return nbuf;
}


static void
set_histogram_type_hook(int newval, void *extra)
{
	if (! histogram_is_dynamic) {
		elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
					  "it's not possible to change the histogram type.");

		HOOK_RETURN(false);
	}

	if (shared_histogram_info) {
		LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);

		shared_histogram_info->type = newval;

		/* if the histogram is logarithmic, there really is not much point
		 * in sending more than 32 bins (or something like that) */
		if (shared_histogram_info->type == HISTOGRAM_LOG) {
			int max_count = (int)ceil(log2(INT_MAX/shared_histogram_info->step));
			if (shared_histogram_info->bins > max_count) {
				elog(NOTICE, "the max bin count %d is too high for log histogram with "
				"%d ms resolution, using %d", shared_histogram_info->bins, shared_histogram_info->step, max_count);
				shared_histogram_info->bins = max_count;
			}
		}

		query_hist_reset(true);
		LWLockRelease(shared_histogram_info->lock);
	}

	HOOK_RETURN(true);
}

static const char *
show_histogram_type_hook(void)
{
	int type = default_histogram_type;

	/* if the histogram is dynamic and was initialized, get value from it */
	if (histogram_is_dynamic && shared_histogram_info)
	{
		LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
		type = shared_histogram_info->type;
		LWLockRelease(shared_histogram_info->lock);
	}

	if (type == HISTOGRAM_LINEAR)
		return "linear";
	else
		return "log";
}


static void
set_histogram_track_utility(bool newval, void *extra)
{
	if (! histogram_is_dynamic) {
		elog(WARNING, "The histogram is not dynamic (query_histogram.dynamic=0), so "
					  "it's not possible to change the histogram type.");

		HOOK_RETURN(false);
	}

	if (shared_histogram_info) {
		LWLockAcquire(shared_histogram_info->lock, LW_EXCLUSIVE);
		shared_histogram_info->track_utility = newval;
		query_hist_reset(true);
		LWLockRelease(shared_histogram_info->lock);
	}

	HOOK_RETURN(true);
}

static const char *
show_histogram_track_utility(void)
{
	bool track_utility = default_histogram_utility;

	/* if the histogram is dynamic and was initialized, get value from it */
	if (histogram_is_dynamic && shared_histogram_info)
	{
		LWLockAcquire(shared_histogram_info->lock, LW_SHARED);
		track_utility = shared_histogram_info->track_utility;
		LWLockRelease(shared_histogram_info->lock);
	}

	if (track_utility)
		return "on";
	else
		return "off";
}

static
size_t get_histogram_size() {
	return MAXALIGN(sizeof(histogram_info_t));
}

/* The histogram is enabled when the number of bins is positive or when
 * the histogram is dynamic (in that case we can't rely on the bins number
 * as it may change next second). */
static
bool query_histogram_enabled() {

	/* when the histogram is static, check the number of bins (does not
	 * make much sense, I guess - it's probably better to remove the
	 * library from the config altogether than just setting 0). */
	if (! default_histogram_dynamic) {
		return (default_histogram_bins > 0);
	}

	return true;
}
