#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "postgres.h"
#include "queryhist.h"
#include "fmgr.h"

#include "funcapi.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(query_histogram);
PG_FUNCTION_INFO_V1(query_histogram_reset);

Datum query_histogram(PG_FUNCTION_ARGS);
Datum query_histogram_reset(PG_FUNCTION_ARGS);

Datum
query_histogram(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    TupleDesc       tupdesc;
    AttInMetadata   *attinmeta;
    histogram_data* data;
        
    /* init on the first call */
    if (SRF_IS_FIRSTCALL()) {
        
        MemoryContext oldcontext;
        
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        data = query_hist_get_data(PG_GETARG_BOOL(0));
        
        /* init (open file, etc.), maybe read all the data in memory
         * so that the file is not kept open for a long time */
        funcctx->user_fctx = data;
        funcctx->max_calls = data->bins_count;
        
        if (data->bins_count > 0) {
            funcctx->max_calls = data->bins_count + 1;
        }
        
        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        /*
         * generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;
        funcctx->tuple_desc = tupdesc;
        
        /* switch back to the old context */
        MemoryContextSwitchTo(oldcontext);
        
    }
    
    /* init the context */
    funcctx = SRF_PERCALL_SETUP();
    
    /* check if we have more data */
    if (funcctx->max_calls > funcctx->call_cntr)
    {
        HeapTuple       tuple;
        Datum           result;
        Datum           values[6];
        bool            nulls[6];
        
        int binIdx;
        
        binIdx = funcctx->call_cntr;
        
        data = (histogram_data*)funcctx->user_fctx;
        
        memset(nulls, 0, sizeof(nulls));
        
        values[0] = UInt32GetDatum(binIdx * data->bins_width);
        
        if (funcctx->max_calls - 1 == funcctx->call_cntr) {
            values[1] = UInt32GetDatum(0);
            nulls[1] = TRUE;
        } else {
            values[1] = UInt32GetDatum((binIdx+1)* data->bins_width);
        }
        
        values[2] = UInt32GetDatum(data->count_data[binIdx]);
        
        if (data->total_count > 0) {
            values[3] = Float4GetDatum(100.0*data->count_data[binIdx] / data->total_count);
        } else {
            values[3] = Float4GetDatum(0);
        }
            
        values[4] = Float4GetDatum(data->time_data[binIdx]);
        
        if (data->total_time > 0) {
            values[5] = Float4GetDatum(100*data->time_data[binIdx] / data->total_time);
        } else {
            values[5] = Float4GetDatum(0);
        }
        
        /* Build and return the tuple. */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        
        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        /* Here we want to return another item: */
        SRF_RETURN_NEXT(funcctx, result);
        
    }
    else
    {
        /* Here we are done returning items and just need to clean up: */
        SRF_RETURN_DONE(funcctx);
    }

}

Datum
query_histogram_reset(PG_FUNCTION_ARGS)
{
    query_hist_reset(false);
    PG_RETURN_VOID();
}
