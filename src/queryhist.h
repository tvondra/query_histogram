#include "postgres.h"
#include "tcop/utility.h"
#include "utils/timestamp.h"

/* TODO When the histogram is static (dynamic=0), we may actually
 *      use less memory because the use can't resize it (so the
 *      bin_count is actually the only possible size). */
/* 1000 bins ought be enough for anyone */
#define HIST_BINS_MAX 1000
#define HISTOGRAM_DUMP_FILE "global/query_histogram.stat"

/* How are the histogram bins scaled? */
typedef enum {
    HISTOGRAM_LINEAR,
    HISTOGRAM_LOG
} histogram_type_t;

/* data types used to store queries */
typedef long long count_bin_t;
typedef float8    time_bin_t;

/* used to transfer the data to the SRF */
typedef struct histogram_data {
    
    int histogram_type;

    unsigned int bins_count;
    unsigned int bins_width;
    
    count_bin_t total_count;
    time_bin_t  total_time;
    
    count_bin_t * count_data;
    time_bin_t  * time_data;
    
} histogram_data;

/* shared segment struct with histogram info (initialized in
 * shmem_startup) */
typedef struct histogram_info_t {
    
    /* lock guarding the histogram */
    LWLockId    lock; 
    
    /* last histogram reset time */
    TimestampTz  last_reset;
    
    /* basic info (number of bins, step (bin width),
     * number of bins, sampling rate */
    int  type;
    int  bins;
    int  step;
    int  sample_pct;
    bool track_utility;
    
    /* data of the histogram */
    count_bin_t count_bins[HIST_BINS_MAX+1];
    time_bin_t  time_bins[HIST_BINS_MAX+1];
    
} histogram_info_t;

histogram_data * query_hist_get_data(bool scale);
void query_hist_reset(bool locked);
TimestampTz get_hist_last_reset(void);
