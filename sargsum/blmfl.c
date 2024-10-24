#include <stdint.h>
#include <math.h>
#include <string.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/errcodes.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "utils/elog.h"
#include "utils/geo_decls.h"
#include "utils/guc.h"
#include "executor/executor.h"

#include "include/bitarray.h"
#include "include/murmur3.h"
#include "include/murmur3.c"

PG_MODULE_MAGIC;

static int BLMFL_M;
static int BLMFL_N;
static int BLMFL_K;

void _PG_init(void) {
    // Define custom GUC parameters

    DefineCustomIntVariable(
        "blmfl.bloomfilter_bitsize",
        "Size of the bloomfilter in bits",
        NULL, &BLMFL_M, 0, 0, INT_MAX, PGC_USERSET, 0, 
        NULL, NULL, NULL);   

    DefineCustomIntVariable(
        "blmfl.estimated_count",
        "Estimated ount of elements in bloomfilter",
        NULL, &BLMFL_N, 0, 0, INT_MAX, PGC_USERSET, 0, 
        NULL, NULL, NULL);    

    DefineCustomIntVariable(
        "blmfl.num_hashes",
        "Number of times the input is hashed",
        NULL, &BLMFL_K, 0, 0, INT_MAX, PGC_USERSET, 0, 
        NULL, NULL, NULL);
}


/********************************************************************************
  bloom filter utils
 ********************************************************************************/

typedef struct
{
    bytea filt;   // Bloomfilter
    int32 m;      // Size of filter (in bits)
    int32 n;      // Expected capacity (i.e. number of elements expected to be inserted into bloomfilter)
    int32 k;      // Number of hash functions
    int32 t;      // Total number of elements
} BlmflResult;

unsigned int unique_count_estimate(int m, int k, BitArray *filt)
{
    int num_set_bits = 0;
    int count = 0;

    for (int i = 0; i < m; i++) {
        if (get_bit(filt, i) == 1){
            num_set_bits++;
        }
    }

    // Calculate the logarithm
    double ratio = (double)num_set_bits / m;  // Cast to double for correct division
    double log_value = log(ratio);

    // Calculate count
    count = -(unsigned int)((m * log_value) / k);    
    return count;
}

unsigned int optimal_k(int m, int n)
{
    return round(log(2) * m / n);
}

double false_positive_rate(int m, int n, int k)
{
    return pow((1 - exp(-1.0 * k * n / m)), (double)k);
}

unsigned int murmur3_hash(char *data, u_int32_t data_length, int i, int m)
{
    uint32_t hashed_data;
    MurmurHash3_x86_32(data, data_length, i, &hashed_data);
    int result = hashed_data % m;
    return result;
}

/********************************************************************************
  function for testing membership of a particular element
 ********************************************************************************/

PG_FUNCTION_INFO_V1(blmfl_test);
Datum blmfl_test(PG_FUNCTION_ARGS)
{
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0); // Header is of type BLMFL_RESULT
    bool isnull;

    bytea *input_data = PG_GETARG_BYTEA_PP(1);
    u_int32_t raw_length = VARSIZE_ANY(input_data) - VARHDRSZ;
    char *raw_data = VARDATA_ANY(input_data);
    
    int32 m = GetAttributeByName(t, "m", &isnull);
    int32 n = GetAttributeByName(t, "n", &isnull);
    int32 k = GetAttributeByName(t, "k", &isnull);

    BitArray *bit_array = to_bit_array(DatumGetByteaP(GetAttributeByName(t, "filt", &isnull))); // The bloomfilter

    bool is_present = true;

    for (int i = 0; i < k; i++) {
        int hash_result = murmur3_hash(raw_data, raw_length, i, m);
        if (get_bit(bit_array, hash_result) == 0){
            is_present = false;
        }
    }

    free_bit_array(bit_array);

    Datum result = BoolGetDatum(is_present);
    PG_RETURN_DATUM(result);
}

/********************************************************************************
  functions for creating and updating the bloomfilter
 ********************************************************************************/

typedef struct
{
    BitArray *filt;
    int32 m; // size of filter (in bits)
    int32 n; // expected capacity
    int32 k; // number of hash functions
    int32 t; // total inserted count
} blmfl_state_t;

static blmfl_state_t *blmfl_state_new_n(MemoryContext aggr_context)
{
    MemoryContext tmp_context = AllocSetContextCreate(aggr_context,
                                                      "blmfl_state",
                                                      ALLOCSET_DEFAULT_MINSIZE,
                                                      ALLOCSET_DEFAULT_INITSIZE,
                                                      ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext old_context = MemoryContextSwitchTo(tmp_context);
    blmfl_state_t *sp = (blmfl_state_t *)palloc(sizeof(blmfl_state_t));
    sp->filt = alloc_bit_array(BLMFL_M);
    sp->m = BLMFL_M;
    sp->n = BLMFL_N;
    sp->k = BLMFL_K;
    sp->t = 0;
    MemoryContextSwitchTo(old_context);
    return sp;
}

static void blmfl_state_add(blmfl_state_t *sp, char *data, u_int32_t data_length)
{
    for (int i = 0; i < sp->k; i++) {
        int hash_result = murmur3_hash(data, data_length, i, sp->m);
        set_bit(sp->filt, hash_result);
    }
    sp->t = sp->t + 1;
    return;
}

// State Function
PG_FUNCTION_INFO_V1(blmfl_sfunc);
Datum blmfl_sfunc(PG_FUNCTION_ARGS)
{
    MemoryContext aggr_context;
    if (!AggCheckCallContext(fcinfo, &aggr_context)){
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("blmfl_sfunc outside transition context")));
    }

    if (PG_NARGS() <= 1){
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("The bloomfilter expects at least 1 data element")));
    }

    blmfl_state_t *sp;
    if (!PG_ARGISNULL(0)) {
        sp = (blmfl_state_t *)(PG_GETARG_POINTER(0));
    } else {
        // On a first call, construct state:
        sp = blmfl_state_new_n(aggr_context);
    }

    bytea *input_data = PG_GETARG_BYTEA_PP(1);
    u_int32_t raw_length = VARSIZE_ANY(input_data) - VARHDRSZ;
    char *raw_data = VARDATA_ANY(input_data);

    blmfl_state_add(sp, raw_data, raw_length);
    
    PG_RETURN_POINTER(sp);
}

// Final Function
PG_FUNCTION_INFO_V1(blmfl_ffunc);
Datum blmfl_ffunc(PG_FUNCTION_ARGS) {
    MemoryContext aggr_context;
    if (!AggCheckCallContext(fcinfo, &aggr_context)) {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("blmfl_ffunc outside transition context")));
    }
    
    if (PG_ARGISNULL(0))
    {
        PG_RETURN_NULL();
    }
    blmfl_state_t *sp = (blmfl_state_t *)(PG_GETARG_POINTER(0));
    if (sp->filt == 0)
    {
        PG_RETURN_NULL();
    }

    const int tup_len = 5;
    TupleDesc tupdesc;
    Datum values[5];
    bool nulls[5];

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));

    tupdesc = CreateTemplateTupleDesc(tup_len);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "filt", BYTEAOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "m", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "n", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "k", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "t", INT4OID, -1, 0);

    tupdesc = BlessTupleDesc(tupdesc);

    values[0] = PointerGetDatum(to_bytea(sp->filt));
    values[1] = Int32GetDatum(sp->m);
    values[2] = Int32GetDatum(sp->n);
    values[3] = Int32GetDatum(sp->k);
    values[4] = Int32GetDatum(sp->t);

    HeapTuple rettuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(rettuple));
}

/********************************************************************************
  functions for getting more information about the bloomfilter
 ********************************************************************************/
PG_FUNCTION_INFO_V1(blmfl_approx_unique_count);
Datum blmfl_approx_unique_count(PG_FUNCTION_ARGS)
{
    bool isnull;
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
    int32 m = GetAttributeByName(t, "m", &isnull);
    int32 k = GetAttributeByName(t, "k", &isnull);

    BitArray *bit_array = to_bit_array(DatumGetByteaP(GetAttributeByName(t, "filt", &isnull)));
    
    int count = unique_count_estimate(m, k, bit_array);
    PG_RETURN_DATUM(Int32GetDatum(count));
}

PG_FUNCTION_INFO_V1(blmfl_optimal_k);
Datum blmfl_optimal_k(PG_FUNCTION_ARGS)
{
    int m = PG_GETARG_INT32(0);
    int n = PG_GETARG_INT32(1);
    
    int k = optimal_k(m, n);
    PG_RETURN_DATUM(Int32GetDatum(k));
}

PG_FUNCTION_INFO_V1(blmfl_fpr);
Datum blmfl_fpr(PG_FUNCTION_ARGS)
{
    bool isnull;
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
    int32 m = GetAttributeByName(t, "m", &isnull);
    int32 n = GetAttributeByName(t, "n", &isnull);
    int32 k = GetAttributeByName(t, "k", &isnull);
    
    float fpr = false_positive_rate(m, n, k);
    PG_RETURN_DATUM(Float8GetDatum(fpr));
}

PG_FUNCTION_INFO_V1(blmfl_merge);
Datum blmfl_merge(PG_FUNCTION_ARGS)
{
    HeapTupleHeader r1 = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTupleHeader r2 = PG_GETARG_HEAPTUPLEHEADER(1);

    bool isnull;
    int32 m1 = GetAttributeByName(r1, "m", &isnull);
    int32 n1 = GetAttributeByName(r1, "n", &isnull);
    int32 k1 = GetAttributeByName(r1, "k", &isnull);
    int32 t1 = GetAttributeByName(r1, "t", &isnull);

    int32 m2 = GetAttributeByName(r2, "m", &isnull);
    int32 n2 = GetAttributeByName(r2, "n", &isnull);
    int32 k2 = GetAttributeByName(r2, "k", &isnull);
    int32 t2 = GetAttributeByName(r2, "t", &isnull);

    BitArray *bit_array1 = to_bit_array(DatumGetByteaP(GetAttributeByName(r1, "filt", &isnull)));
    BitArray *bit_array2 = to_bit_array(DatumGetByteaP(GetAttributeByName(r2, "filt", &isnull)));

    const int size = bit_array1->size;
    BitArray *bit_array_n = alloc_bit_array(size);
    for (int i = 0; i < size; i++)
        if (get_bit(bit_array1, i) == 1 || get_bit(bit_array2, i) == 1)
            set_bit(bit_array_n, i);

    const int tup_len = 5;
    TupleDesc tupdesc;
    Datum values[5];
    bool nulls[5];

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));

    tupdesc = CreateTemplateTupleDesc(tup_len);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "filt", BYTEAOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "m", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "n", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "k", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "t", INT4OID, -1, 0);

    tupdesc = BlessTupleDesc(tupdesc);

    values[0] = PointerGetDatum(to_bytea(bit_array_n));
    values[1] = Int32GetDatum(m1);
    values[2] = Int32GetDatum(n1+n2);
    values[3] = Int32GetDatum(k1);
    values[4] = Int32GetDatum(t1+t2);

    HeapTuple rettuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(rettuple));
}


/********************************************************************************
  TEMPORARY BUG FIX W/ INTS ONLY BLMFL
 ********************************************************************************/
// Int hash (using murmur3 library)
uint32_t hash_int(int64_t key, int i, int m) {
    uint32_t hash_value;
    MurmurHash3_x86_32(&key, sizeof(key), i, &hash_value);
    int result = hash_value % m;
    return result;
}


// Testing for elements in bloomfilter
PG_FUNCTION_INFO_V1(blmfl_test_int);
Datum blmfl_test_int(PG_FUNCTION_ARGS)
{
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0); // Header is of type BLMFL_RESULT
    bool isnull;
    
    int32 m = GetAttributeByName(t, "m", &isnull);
    int32 n = GetAttributeByName(t, "n", &isnull);
    int32 k = GetAttributeByName(t, "k", &isnull);

    int64_t input_data = PG_GETARG_INT64(1);

    BitArray *bit_array = to_bit_array(DatumGetByteaP(GetAttributeByName(t, "filt", &isnull))); // The bloomfilter

    bool is_present = true;

    for (int i = 0; i < k; i++) {
        int hash_result = hash_int(input_data, i, m);
        if (get_bit(bit_array, hash_result) == 0){
            is_present = false;
        }
    }

    free_bit_array(bit_array);

    Datum result = BoolGetDatum(is_present);
    PG_RETURN_DATUM(result);
}

// Add to blmfl
static void blmfl_state_add_int(blmfl_state_t *sp, uint64_t data)
{
    for (int i = 0; i < sp->k; i++) {
        int hash_result = hash_int(data, i, sp->m);
        set_bit(sp->filt, hash_result);
    }
    sp->t = sp->t + 1;
    return;
}

// State Function
PG_FUNCTION_INFO_V1(blmfl_sfunc_int);
Datum blmfl_sfunc_int(PG_FUNCTION_ARGS)
{
  MemoryContext aggr_context;
  if (!AggCheckCallContext(fcinfo, &aggr_context))
  {
    ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
             errmsg("blmfl_sfunc outside transition context")));
  }
  blmfl_state_t *sp;
  if (!PG_ARGISNULL(0))
  {
    sp = (blmfl_state_t *)(PG_GETARG_POINTER(0));
  }
  else
  {
    // on a first call, construct state:
    int32 m = PG_GETARG_UINT32(2); // size of filter
    int32 n = PG_GETARG_UINT32(3); // expected capacity
    sp = blmfl_state_new_n(aggr_context);
  }
  if (!PG_ARGISNULL(1))
  {
    blmfl_state_add_int(sp, PG_GETARG_INT64(1));
  }
  PG_RETURN_POINTER(sp);
}