
typedef struct histogram_data {

    unsigned int bins_count;
    unsigned int bins_width;
    
    unsigned long total_count;
    unsigned long total_time;
    
    unsigned long * count_data;
    unsigned long * time_data;
    
} histogram_data;

void query_hist_init(int bins, int step);
void query_hist_reset(int bins);
void query_hist_free();

void query_hist_add_query(int duration);

int  query_hist_get_bins();
int  query_hist_get_step();

histogram_data * query_hist_get_data();