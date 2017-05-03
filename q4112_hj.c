#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

typedef struct {
  uint32_t key;
  uint32_t val;
} bucket_t;

typedef struct {
  pthread_barrier_t barrier;
  pthread_t id;
  int thread;
  int threads;
  size_t buckets;
  size_t inner_tuples;
  size_t outer_tuples;
  int8_t log_buckets;
  const uint32_t* inner_keys;
  const uint32_t* inner_vals;
  const uint32_t* outer_keys;
  const uint32_t* outer_vals;
  uint64_t sum;
  uint32_t count;
  bucket_t* table;
} q4112_run_info_t;

// use global variable barrier
pthread_barrier_t barrier; 

void* q4112_run_thread(void* arg) {
  q4112_run_info_t* info = (q4112_run_info_t*) arg;
  assert(pthread_equal(pthread_self(), info->id));

  // copy info
  bucket_t* table = info->table;
  int8_t log_buckets = info->log_buckets;
  size_t buckets = info->buckets;
  size_t thread  = info->thread;
  size_t threads = info->threads;
  size_t inner_tuples = info->inner_tuples;
  size_t outer_tuples = info->outer_tuples;
  const uint32_t* inner_keys = info->inner_keys;
  const uint32_t* inner_vals = info->inner_vals;
  const uint32_t* outer_keys = info->outer_keys;
  const uint32_t* outer_vals = info->outer_vals;

  // thread boundaries for inner table
  size_t inner_beg = (inner_tuples / threads) * (thread + 0);
  size_t inner_end = (inner_tuples / threads) * (thread + 1);
  if (thread + 1 == threads) 
    inner_end = inner_tuples;

  // scan whole inner table but split outer table
  size_t i, o, h;
  for (i = inner_beg; i < inner_end; ++i) {
    uint32_t key = inner_keys[i];
    uint32_t val = inner_vals[i];

    // multiplicative hashing
    h = (uint32_t) (key * 0x9e3779b1);
    h >>= 32 - log_buckets;

    // search for empty bucket
    // flag == 1 means insert this key successfully
    size_t flag; 
    flag = 0;
    while (flag == 0) {
      if (table[h].key == 0) {
        if (__sync_bool_compare_and_swap(&table[h].key, 0, key)) { 
          table[h].val = val;
          flag = 1;
        }
      }

      if (flag == 0) {
        // go to next bucket (linear probing)
        h = (h + 1) & (buckets - 1);
      }
    }

  }

  // Here the first part(inner part) finished.
  // All threads wait here
  pthread_barrier_wait(&barrier);

  // After all thread finished first part(inner part), they start the 
  // second part(outer part) at the same time.

  // thread boundaries for inner table
  size_t outer_beg = (outer_tuples / threads) * (thread + 0);
  size_t outer_end = (outer_tuples / threads) * (thread + 1);
  if (thread + 1 == threads) 
    outer_end = outer_tuples;

  // initialize single aggregate
  uint32_t count = 0;
  uint64_t sum = 0;

  // probe outer table using hash table
  for (o = outer_beg; o < outer_end; ++o) {
    uint32_t key = outer_keys[o];

    // multiplicative hashing
    h = (uint32_t) (key * 0x9e3779b1);
    h >>= 32 - log_buckets;

    // search for matching bucket
    uint32_t tab = table[h].key;
    while (tab != 0) {
      // keys match
      if (tab == key) {   
        // update single aggregate
        sum += table[h].val * (uint64_t) outer_vals[o];
        count += 1;
        // guaranteed single match (join on primary key)
        break;
      }

      // go to next bucket (linear probing)
      h = (h + 1) & (buckets - 1);
      tab = table[h].key;
    }
  }

  // get result
  info->sum = sum;
  info->count = count;
  pthread_exit(NULL);
}

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

  // malloc
  q4112_run_info_t* info = (q4112_run_info_t*)
      malloc(threads * sizeof(q4112_run_info_t));
  assert(info != NULL);

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
  bucket_t* table = (bucket_t*) calloc(buckets, sizeof(bucket_t)); // * here bucket_t* represents array ?
  assert(table != NULL);

  // set barrier
  pthread_barrier_init(&barrier, NULL, threads);

  for (t = 0; t < threads; ++t) {
    info[t].thread = t;
    info[t].threads = threads;
    info[t].inner_keys = inner_keys;
    info[t].inner_vals = inner_vals;
    info[t].outer_keys = outer_join_keys;
    info[t].outer_vals = outer_vals;
    info[t].inner_tuples = inner_tuples;
    info[t].outer_tuples = outer_tuples;
    info[t].log_buckets = log_buckets;
    info[t].table = table;
    info[t].buckets = buckets;
    pthread_create(&info[t].id, NULL, q4112_run_thread, &info[t]);
  }

  // gather result
  uint64_t sum = 0;
  uint32_t count = 0;
  for (t = 0; t < threads; ++t) {
    pthread_join(info[t].id, NULL);
    sum += info[t].sum;
    count += info[t].count;
  }

  // cleanup and return average (integer division)
  free(table);
  free(info);

  return sum / count;
}




