#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "postgres.h"
#include "queryhist.h"
#include "fmgr.h"

#include "funcapi.h"

#if (PG_VERSION_NUM >= 90300)
#include "access/htup_details.h"
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(query_histogram);
PG_FUNCTION_INFO_V1(query_histograms);

PG_FUNCTION_INFO_V1(query_histogram_reset);
PG_FUNCTION_INFO_V1(query_histogram_reset_global);
PG_FUNCTION_INFO_V1(query_histogram_reset_db);

PG_FUNCTION_INFO_V1(query_histogram_get_version);
PG_FUNCTION_INFO_V1(query_histogram_get_reset_timestamp);

Datum query_histogram(PG_FUNCTION_ARGS);
Datum query_histograms(PG_FUNCTION_ARGS);

Datum query_histogram_reset(PG_FUNCTION_ARGS);
Datum query_histogram_reset_global(PG_FUNCTION_ARGS);
Datum query_histogram_reset_db(PG_FUNCTION_ARGS);

Datum query_histogram_get_version(PG_FUNCTION_ARGS);
Datum query_histogram_get_reset_timestamp(PG_FUNCTION_ARGS);

Datum
query_histogram(PG_FUNCTION_ARGS)
{
    FuncCallContext    *funcctx;
    TupleDesc           tupdesc;
    AttInMetadata      *attinmeta;
    histogram_data     *data;

    /* parameters */
    bool                scale       = PG_GETARG_BOOL(0);
    Oid                 databaseoid = PG_GETARG_OID(1);

    /* init on the first call */
    if (SRF_IS_FIRSTCALL()) {

        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* no database OID supplied => global histogram */
        if (PG_ARGISNULL(1))
            data = histogram_get_data_global(scale);
        else
            data = histogram_get_data_db(scale, databaseoid);

        if (data != NULL) {
            funcctx->user_fctx = data;
            funcctx->max_calls = data->bins_count;

            if (data->bins_count > 0)
                funcctx->max_calls = data->bins_count + 1;
        } else {
            funcctx->max_calls = 0;
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

        if (data->histogram_type == HISTOGRAM_LINEAR) {

            values[0] = UInt32GetDatum(binIdx * data->bins_width);

            if (funcctx->max_calls - 1 == funcctx->call_cntr) {
                values[1] = UInt32GetDatum(0);
                nulls[1] = TRUE;
            } else {
                values[1] = UInt32GetDatum((binIdx+1)* data->bins_width);
            }
        } else {

            if (funcctx->call_cntr == 0) {
                values[0] = UInt32GetDatum(0);
            } else {
                values[0] = UInt32GetDatum(pow(2,binIdx-1) * data->bins_width);
            }

            if (funcctx->max_calls - 1 == funcctx->call_cntr) {
                values[1] = UInt32GetDatum(0);
                nulls[1] = TRUE;
            } else {
                values[1] = UInt32GetDatum(pow(2,binIdx) * data->bins_width);
            }

        }

        values[2] = Int64GetDatum(data->count_data[binIdx]);

        if (data->total_count > 0) {
            values[3] = Float4GetDatum(100.0*data->count_data[binIdx] / data->total_count);
        } else {
            values[3] = Float4GetDatum(0);
        }

        values[4] = Float8GetDatum(data->time_data[binIdx]);

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
query_histograms(PG_FUNCTION_ARGS)
{
    FuncCallContext    *funcctx;
    TupleDesc           tupdesc;
    AttInMetadata      *attinmeta;
    histogram_data     *data;
    int                 dbcount;

    bool                scale = PG_GETARG_BOOL(0);

    /* init on the first call */
    if (SRF_IS_FIRSTCALL()) {

        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        data = histogram_get_data_dbs(scale, &dbcount);

        if (data != NULL) {
            funcctx->user_fctx = data;
            funcctx->max_calls = data->bins_count;

            if (data->bins_count > 0)
                funcctx->max_calls = data->bins_count + 1;
        } else {
            funcctx->max_calls = 0;
        }

        /* same number of bins for each database, and we have 'dbcount' dbs */
        funcctx->max_calls *= dbcount;

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
        HeapTuple   tuple;
        Datum       result;
        Datum       values[7];
        bool        nulls[7];

        /* index of the bin */
        int binIdx;

        /* data stored in the */
        data = (histogram_data*)funcctx->user_fctx;

        binIdx = (funcctx->call_cntr % (data->bins_count+1));

        /* if we reached end of this database histogram, skip to the next data item */
        if ((funcctx->call_cntr > 0) && (binIdx == 0)) {
            funcctx->user_fctx = data->next;
            /* TODO free the old data? or just wait until the whole query context gets freed */
            data = (histogram_data*)funcctx->user_fctx;
        }

        memset(nulls, 0, sizeof(nulls));

        /* store OID of the database */
        if (data->databaseoid == InvalidOid)
            nulls[0] = 1;
        else
            values[0] = ObjectIdGetDatum(data->databaseoid);

        if (data->histogram_type == HISTOGRAM_LINEAR) {

            values[1] = UInt32GetDatum(binIdx * data->bins_width);

            /* last bin in the histogram has empty (NULL) upper bound */
            if (data->bins_count == binIdx) {
                values[2] = UInt32GetDatum(0);
                nulls[2] = TRUE;
            } else {
                values[2] = UInt32GetDatum((binIdx+1)* data->bins_width);
            }

        } else {

            /* first bin has zero lower bound */
            if (funcctx->call_cntr == 0) {
                values[1] = UInt32GetDatum(0);
            } else {
                values[1] = UInt32GetDatum(pow(2,binIdx-1) * data->bins_width);
            }

            /* last bin in the histogram has empty (NULL) upper bound */
            if (data->bins_count - 1 == binIdx) {
                values[2] = UInt32GetDatum(0);
                nulls[2] = TRUE;
            } else {
                values[2] = UInt32GetDatum(pow(2,binIdx) * data->bins_width);
            }

        }

        values[3] = Int64GetDatum(data->count_data[binIdx]);

        if (data->total_count > 0) {
            values[4] = Float4GetDatum(100.0*data->count_data[binIdx] / data->total_count);
        } else {
            values[4] = Float4GetDatum(0);
        }

        values[5] = Float8GetDatum(data->time_data[binIdx]);

        if (data->total_time > 0) {
            values[6] = Float4GetDatum(100*data->time_data[binIdx] / data->total_time);
        } else {
            values[6] = Float4GetDatum(0);
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
    bool remove     = PG_GETARG_BOOL(0);

    histogram_reset(false, remove);
    PG_RETURN_VOID();
}

Datum
query_histogram_reset_global(PG_FUNCTION_ARGS)
{
    histogram_reset_global(false);
    PG_RETURN_VOID();
}

Datum
query_histogram_reset_db(PG_FUNCTION_ARGS)
{

    Oid databaseoid = PG_GETARG_OID(0);
    bool remove     = PG_GETARG_BOOL(1);
    bool result;

    result = histogram_reset_db(false, databaseoid, remove);

    PG_RETURN_BOOL(result);

}

Datum
query_histogram_get_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(histogram_version());
}

Datum
query_histogram_get_reset_timestamp(PG_FUNCTION_ARGS)
{
    TimestampTz reset;
    bool        found = TRUE;
    Oid         databaseoid = PG_GETARG_OID(0);

    if (PG_ARGISNULL(0))
        reset = histogram_get_reset_global();
    else
        reset = histogram_get_reset_db(databaseoid, &found);

    if (found)
        PG_RETURN_TIMESTAMP(reset);
    else
        PG_RETURN_NULL();

}
