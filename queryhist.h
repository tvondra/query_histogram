#include "postgres.h"
#include "tcop/utility.h"

/* 1000 bins ought be enough for anyone */
#define HIST_BINS_MAX 1000

/* How are the histogram bins scaled? */
typedef enum {
    HISTOGRAM_LINEAR,
    HISTOGRAM_LOG
} histogram_type_t;

/* data types used to store queries (maybe we should use
 * longer values) */
typedef unsigned long count_bin_t;
typedef float4        time_bin_t;

/* used to transfer the data to the SRF */
typedef struct histogram_data {

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
    
    /* basic info (number of bins, step (bin width),
     * number of bins, sampling rate */
    int  type;
    int  bins;
    int  step;
    int  sample_pct;
    
    /* data of the histogram */
    count_bin_t count_bins[HIST_BINS_MAX+1];
    time_bin_t  time_bins[HIST_BINS_MAX+1];
    
} histogram_info_t;

histogram_data * query_hist_get_data(bool scale);
void query_hist_reset(bool locked);
