#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

// bucket representation for hash table
typedef struct {
  uint32_t key;
  uint32_t val;
} bucket_t;

// bucket representation for global aggregation table
typedef struct {
  uint32_t key;
  uint64_t sum;
  uint32_t count;
} bucket_aggr_t;


// thread info structure for creating threads and transferring useful
// information
typedef struct {
  pthread_t id;
  int thread;
  int threads;
  size_t inner_tuples;
  size_t outer_tuples;
  const uint32_t* inner_keys;
  const uint32_t* inner_vals;
  const uint32_t* outer_keys;
  const uint32_t* outer_vals;
  const uint32_t* outer_aggr_keys;
  uint64_t sum;
  uint32_t count;
  uint64_t sum_avgs;
  uint32_t num_groups;
  bucket_t* table;  // not const since table is mutable
  int8_t log_buckets;
  size_t buckets;
  bucket_aggr_t* aggr_table;
  size_t aggr_buckets;
  int8_t log_aggr_buckets;
} q4112_run_info_hj_t;

typedef struct {
  pthread_t id;
  int thread;
  int threads;
  size_t outer_tuples;
  const uint32_t* outer_aggr_keys;
  int8_t log_partitions;
  size_t partitions;
  uint32_t* bitmaps;
  size_t sum;
} q4112_estimation_info_hj_t;

// the barrier to control the threads
static pthread_barrier_t barrier;
static pthread_barrier_t barrier2;
static pthread_barrier_t barrier3;


uint32_t trailing_zero_count(uint32_t bitmap) {
  if (bitmap == 0) {
    return -1;
  }
  uint32_t count = 0;
  while (bitmap % 2 == 0) {
    count++;
    bitmap >>= 1;
  }
  return count;
}

uint8_t trailing_zero_count2(size_t size) {
  if (size == 0) {
    return -1;
  }
  uint32_t count = 0;
  while (size % 2 == 0) {
    count++;
    size >>= 1;
  }
  return count;
}

void* estimate_thread(void* arg) {
  q4112_estimation_info_hj_t* info = (q4112_estimation_info_hj_t*) arg;
  assert(pthread_equal(pthread_self(), info->id));

  // copy info from thread info
  size_t thread  = info->thread;
  size_t threads = info->threads;
  size_t outer_tuples = info->outer_tuples;
  int8_t log_partitions = info->log_partitions;
  size_t partitions = info->partitions;
  uint32_t* bitmaps = info->bitmaps;

  const uint32_t* outer_aggr_keys = info->outer_aggr_keys;

  uint32_t* bitmaps_local = calloc(partitions, 4);

  
  size_t aggr_keys_beg = (outer_tuples / threads) * (thread + 0);
  size_t aggr_keys_end = (outer_tuples / threads) * (thread + 1);
  if (thread + 1 == threads) {
    aggr_keys_end = outer_tuples;
  }

  size_t i;
  for (i = aggr_keys_beg; i != aggr_keys_end; ++i) {
    uint32_t h = (uint32_t) (outer_aggr_keys[i] * 0x9e3779b1);
    size_t p = h & (partitions - 1);  // use some hash bits to partition
    h >>= log_partitions;  // use remaining hash bits for the bitmap
    bitmaps_local[p] |= h & -h;  // update bitmap of partition
  }

  for (i = 0; i != partitions; ++i) {
    if (bitmaps_local[i] != 0){
      __sync_fetch_and_or(&bitmaps[i], bitmaps_local[i]);
    }
  }

  pthread_barrier_wait(&barrier);

  // set thread boundaries for outer table
  size_t partitions_beg = (partitions / threads) * (thread + 0);
  size_t partitions_end = (partitions / threads) * (thread + 1);
  // fix boundary for last thread
  if (thread + 1 == threads) partitions_end = partitions;

  size_t sum = 0;
  for (i = partitions_beg; i != partitions_end; ++i) {
    sum += ((size_t) 1) << trailing_zero_count(~bitmaps[i]);
  }
  info->sum = sum;
  free(bitmaps_local);
  pthread_exit(NULL);
}


size_t estimate(const uint32_t* outer_aggr_keys, size_t outer_tuples, int threads) {
  const int8_t log_partitions = 12;
  size_t t, partitions = 1 << log_partitions;
  uint32_t* bitmaps = calloc(partitions, 4);

  // allocate threads info
  q4112_estimation_info_hj_t* info = (q4112_estimation_info_hj_t*)
      malloc(threads * sizeof(q4112_estimation_info_hj_t));
  assert(info != NULL);

  for (t = 0; t < threads; t++) {
    info[t].thread = t;
    info[t].threads = threads;
    info[t].outer_aggr_keys = outer_aggr_keys;
    info[t].outer_tuples = outer_tuples;
    info[t].partitions = partitions;
    info[t].log_partitions = log_partitions;
    info[t].bitmaps = bitmaps;
    pthread_create(&info[t].id, NULL, estimate_thread, &info[t]);
  }

  size_t sum = 0;
  for (t = 0; t != threads; ++t) {
    pthread_join(info[t].id, NULL);
    sum += info[t].sum;
  }
  free(bitmaps);
  return sum / 0.77351;
}

// build hash table and probe to get result (each thread has it own boundaries)
void* q4112_run_thread(void* arg) {
  q4112_run_info_hj_t* info = (q4112_run_info_hj_t*) arg;
  assert(pthread_equal(pthread_self(), info->id));

  // copy info from thread info
  size_t thread  = info->thread;
  size_t threads = info->threads;
  size_t inner_tuples = info->inner_tuples;
  size_t outer_tuples = info->outer_tuples;
  int8_t log_buckets = info->log_buckets;
  size_t buckets = info->buckets;
  int8_t log_aggr_buckets = info->log_aggr_buckets;
  size_t aggr_buckets = info->aggr_buckets;

  const uint32_t* inner_keys = info->inner_keys;
  const uint32_t* inner_vals = info->inner_vals;
  const uint32_t* outer_keys = info->outer_keys;
  const uint32_t* outer_vals = info->outer_vals;
  const uint32_t* outer_aggr_keys = info->outer_aggr_keys;
  bucket_t* table = info->table;
  bucket_aggr_t* aggr_table = info->aggr_table;

  // set thread boundaries for inner table
  size_t inner_beg = (inner_tuples / threads) * (thread + 0);
  size_t inner_end = (inner_tuples / threads) * (thread + 1);
  if (thread + 1 == threads) {
    inner_end = inner_tuples;
  }

  // build inner table into hash table
  size_t i, o, h;
  for (i = inner_beg; i != inner_end; ++i) {
    uint32_t key = inner_keys[i];
    uint32_t val = inner_vals[i];

    // multiplicative hashing
    h = (uint32_t) (key * 0x9e3779b1);
    h >>= 32 - log_buckets;

    // search for empty bucket in hash table and insert data
    int written_successful = 0;
    while (!written_successful) {
      uint32_t old_key = table[h].key;
      // use compare-and-swap and try to modify key and value
      if (old_key == 0 &&
          __sync_bool_compare_and_swap(&(table[h].key), old_key, key)) {
        table[h].val = val;
        written_successful = 1;
      } else {  // failed to write key and value
        // move to next available bucket
        h = (h + 1) & (buckets - 1);
      }
    }
  }

  // barrier wait for next stage: matching
  pthread_barrier_wait(&barrier2);

  // set thread boundaries for outer table
  size_t outer_beg = (outer_tuples / threads) * (thread + 0);
  size_t outer_end = (outer_tuples / threads) * (thread + 1);
  if (thread + 1 == threads) {
    outer_end = outer_tuples;
  }

  int small_hash_count = 0;
  bucket_aggr_t* smallhash = (bucket_aggr_t*) calloc(512, sizeof(bucket_aggr_t));

  // probe outer table using hash table
  for (o = outer_beg; o != outer_end; ++o) {
    uint32_t key = outer_keys[o];
    uint32_t aggr_key = outer_aggr_keys[o];

    // multiplicative hashing
    h = (uint32_t) (key * 0x9e3779b1);
    h >>= 32 - log_buckets;

    // search for matching bucket
    uint32_t tab = table[h].key;

    while (tab != 0) {
      // keys match
      if (tab == key) {
        // hashing for small hash table
        size_t h2;
        h2 = (uint32_t) (aggr_key * 0x9e3779b1);
        h2 >>= 32 - 9;

        while (smallhash[h2].key != 0 && smallhash[h2].key != aggr_key) {
          h2 = (h2 + 1) & (512 - 1);
        }

        if (smallhash[h2].key == 0) {
          smallhash[h2].key = aggr_key;
          small_hash_count += 1;
        }

        smallhash[h2].count += 1;
        smallhash[h2].sum += table[h].val * (uint64_t) outer_vals[o];

        if (small_hash_count == 512 || o == outer_end - 1) {
          size_t i, aggr_h;
          uint32_t aggr_key;

          for (i = 0; i < 512; i++) {
            if (smallhash[i].key == 0) {
              continue;
            }
            aggr_key = smallhash[i].key;

            aggr_h = (uint32_t) (aggr_key * 0x9e3779b1);
            aggr_h >>= 32 - log_aggr_buckets;

            int flag = 0;
            while (flag == 0) {
              // if already occupied
              if (aggr_table[aggr_h].key == aggr_key) {
                flag = 1;
              } else { // if not occupied, try to occupy
                if (__sync_bool_compare_and_swap(&(aggr_table[aggr_h].key), 0, aggr_key)) {
                  flag = 1;
                } else if (aggr_table[aggr_h].key == aggr_key) { // if failed to occupy, check if occpuied by the same group
                  flag = 1;
                }
              }
              if (flag == 0) {
                aggr_h = (aggr_h + 1) & (aggr_buckets - 1);

              }
            }

            __sync_fetch_and_add(&aggr_table[aggr_h].sum, smallhash[i].sum);
            __sync_fetch_and_add(&aggr_table[aggr_h].count, smallhash[i].count);

            smallhash[i].key = 0;
            smallhash[i].count = 0;
            smallhash[i].sum = 0;
          }

          small_hash_count = 0;
        }
        
        break;
      }
      // go to next bucket (linear probing)
      h = (h + 1) & (buckets - 1);
      tab = table[h].key;
    }
  }

  free(smallhash);

  // barrier wait for next stage: summing up
  pthread_barrier_wait(&barrier3);

  // set thread boundaries for global aggregate table
  size_t aggr_beg = (aggr_buckets / threads) * (thread + 0);
  size_t aggr_end = (aggr_buckets / threads) * (thread + 1);
  if (thread + 1 == threads) {
    aggr_end = aggr_buckets;
  }

  uint64_t sum_avgs = 0;
  uint32_t num_groups = 0;
  for (i = aggr_beg; i != aggr_end; ++i) {
    if (aggr_table[i].key != 0) {
      sum_avgs += aggr_table[i].sum / aggr_table[i].count;
      num_groups += 1;
    }
  }

  // save results
  info->sum_avgs = sum_avgs;
  info->num_groups = num_groups;
  pthread_exit(NULL);
}

size_t smallest_power_of_2_greater_equal_n(size_t n) {
  size_t ans = 1;
  while (ans < n) {
    ans *= 2;
  }
  return ans;
}

// the function to start multi-threaded hash join for the query
uint64_t q4112_run(
    const uint32_t* inner_keys,
    const uint32_t* inner_vals,
    size_t inner_tuples,
    const uint32_t* outer_join_keys,
    const uint32_t* outer_aggr_keys,
    const uint32_t* outer_vals,
    size_t outer_tuples,
    int threads) {
  // check number of threads
  int t, max_threads = sysconf(_SC_NPROCESSORS_ONLN);
  
  assert(max_threads > 0 && threads > 0 && threads <= max_threads);

  // allocate threads info
  q4112_run_info_hj_t* info = (q4112_run_info_hj_t*)
      malloc(threads * sizeof(q4112_run_info_hj_t));
  assert(info != NULL);

  // estimate the global aggregation table size
  pthread_barrier_init(&barrier, NULL, threads);
  size_t aggr_buckets_estimate = estimate(outer_aggr_keys, outer_tuples, threads);

  size_t aggr_buckets = 1;
  while (aggr_buckets < aggr_buckets_estimate) {
    aggr_buckets *= 2;
  }

  int8_t log_aggr_buckets = trailing_zero_count2(aggr_buckets);

  // allocate and initialize the global aggregation table
  bucket_aggr_t* aggr_table = (bucket_aggr_t*) calloc(aggr_buckets, sizeof(bucket_aggr_t));
  assert(aggr_table != NULL);

  // set the number of hash table buckets to be 2^k
  // the hash table fill rate will be between 1/3 and 2/3
  int8_t log_buckets = 1;
  size_t buckets = 2;
  while (buckets * 0.67 < inner_tuples) {
    log_buckets += 1;
    buckets += buckets;
  }

  // allocate and initialize the hash table
  // there are no 0 keys (see header) so we use 0 for "no key"
  bucket_t* table = (bucket_t*) calloc(buckets, sizeof(bucket_t));
  assert(table != NULL);


  // set up barrier for threads
  pthread_barrier_init(&barrier2, NULL, threads);
  pthread_barrier_init(&barrier3, NULL, threads);

  // run threads for matching
  for (t = 0; t != threads; ++t) {
    info[t].thread = t;
    info[t].threads = threads;
    info[t].inner_keys = inner_keys;
    info[t].inner_vals = inner_vals;
    info[t].outer_keys = outer_join_keys;
    info[t].outer_vals = outer_vals;
    info[t].outer_aggr_keys = outer_aggr_keys;
    info[t].inner_tuples = inner_tuples;
    info[t].outer_tuples = outer_tuples;
    info[t].table = table;
    info[t].log_buckets = log_buckets;
    info[t].buckets = buckets;
    info[t].aggr_table = aggr_table;
    info[t].log_aggr_buckets = log_aggr_buckets;
    info[t].aggr_buckets = aggr_buckets;
    pthread_create(&info[t].id, NULL, q4112_run_thread, &info[t]);
  }

  // gather result
  uint64_t sum_avgs = 0;
  uint32_t num_groups = 0;
  for (t = 0; t != threads; ++t) {
    pthread_join(info[t].id, NULL);
    sum_avgs += info[t].sum_avgs;
    num_groups += info[t].num_groups;
  }

  // clean up
  pthread_barrier_destroy(&barrier);
  pthread_barrier_destroy(&barrier2);
  pthread_barrier_destroy(&barrier3);
  free(info);
  free(table);
  free(aggr_table);

  return sum_avgs / num_groups;
}