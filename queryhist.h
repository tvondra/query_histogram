#include "postgres.h"

typedef struct histogram_data {

    unsigned int bins_count;
    unsigned int bins_width;
    
    unsigned long total_count;
    float4        total_time;
    
    unsigned long * count_data;
    float4        * time_data;
    
} histogram_data;

/* How are the histogram bins scaled? */
typedef enum {
    HISTOGRAM_LINEAR,
    HISTOGRAM_LOG
} histogram_type_t;

histogram_data * query_hist_get_data(bool scale);
void query_hist_reset(bool locked);
void query_hist_refresh();