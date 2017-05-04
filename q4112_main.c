#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>
#include "q4112.h"

uint64_t real_time(void) {
  struct timespec t;
  assert(clock_gettime(CLOCK_REALTIME, &t) == 0);
  return t.tv_sec * 1000 * 1000 * 1000 + t.tv_nsec;
}

const char* add_commas(uint64_t x) {
  static char buf[32];
  int digit = 0;
  size_t i = sizeof(buf) / sizeof(char);
  buf[--i] = '\0';
  do {
    if (digit++ == 3) {
      buf[--i] = ',';
      digit = 1;
    }
    buf[--i] = (x % 10) + '0';
    x /= 10;
  } while (x);
  return &buf[i];
}

int main(int argc, char* argv[])
{
  printf("In the main function of entrance.\n");
  FILE* fd = fopen("q4112_hj.csv", "w+");


  size_t inner_tuples[] = {100,       100,       100,       100,       100,       100,       100, 
                           100000,    100000,    100000,    100000,    100000,    100000,    100000,
                           100000000, 100000000, 100000000, 100000000, 100000000, 100000000, 100000000};

  double inner_selectivity[] = {1.0, 1.0, 1.0, 1.0, 1.0, 
                                1.0, 1.0, 1.0, 1.0, 1.0, 
                                1.0, 1.0, 1.0, 1.0, 1.0, 
                                1.0, 1.0, 1.0, 1.0, 1.0, 
                                1.0};
  
  uint32_t inner_val_max[] = {99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999};
  
  size_t outer_tuples[] = {1000000000, 1000000000, 1000000000, 1000000000,
                           1000000000, 1000000000, 1000000000, 1000000000,
                           1000000000, 1000000000, 1000000000, 1000000000,
                           1000000000, 1000000000, 1000000000, 1000000000,
                           1000000000, 1000000000, 1000000000, 1000000000,
                           10000000};

  double outer_selectivity[] = {1.0, 1.0, 1.0, 1.0, 1.0, 
                               1.0, 1.0, 1.0, 1.0, 1.0, 
                               1.0, 1.0, 1.0, 1.0, 1.0, 
                               1.0, 1.0, 1.0, 1.0, 1.0, 
                               1.0};

  uint32_t outer_val_max[] = {99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999, 99999, 99999, 99999,
                              99999};

  size_t groups[] = {100, 10000, 1000000, 100000000, 100000000, 100000000, 100000000,
                     100, 10000, 1000000, 100000000, 100000000, 100000000, 100000000,
                     100, 10000, 1000000, 100000000, 100000000, 100000000, 100000000};

  size_t hh_groups[] = {0, 0, 0, 0, 100, 100, 10000,
                        0, 0, 0, 0, 100, 100, 10000,
                        0, 0, 0, 0, 100, 100, 10000};

  double hh_probability[] = {0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 1.0,
                             0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 1.0,
                             0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 1.0};

  int threads[5] = {1, 2, 4, 8, 16};

  // print header
  fprintf(fd, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n","inner_tuples", "inner_selectivity", "inner_val_max",
                "outer_tuples", "outer_selectivity", "outer_val_max",
                "groups", "hh_groups", "hh_probability",
                "threads", "repeat", "nanoseconds");

  int s,t,repeat;
  for (s = 20; s < 21; ++s) {
    // allocate space for outer table
    uint32_t *outer_join_keys = (uint32_t *)malloc(outer_tuples[s] * 4);
    assert(outer_join_keys != NULL);

    //        uint64_t gen_ns = real_time();
    // allocate space for inner table
    uint32_t *inner_keys = (uint32_t *)malloc(inner_tuples[s] * 4);
    assert(inner_keys != NULL);
    uint32_t *inner_vals = (uint32_t *)malloc(inner_tuples[s] * 4);
    assert(inner_vals != NULL);
    
    uint32_t *outer_aggr_keys = NULL;
    if (groups[s] > 0)
    { 
      outer_aggr_keys = (uint32_t *)malloc(outer_tuples[s] * 4);
      assert(outer_aggr_keys != NULL);
    }
    uint32_t *outer_vals = (uint32_t *)malloc(outer_tuples[s] * 4);
    assert(outer_vals != NULL);

    
    uint64_t gen_res = q4112_gen(inner_keys, inner_vals, inner_tuples[s],
                                 inner_selectivity[s], inner_val_max[s],
                                 outer_join_keys, outer_aggr_keys, outer_vals, outer_tuples[s],
                                 outer_selectivity[s], outer_val_max[s], groups[s], hh_groups[s], hh_probability[s]);

    for (t = 2; t < 3; ++t) {
      for (repeat = 1; repeat <= 1; ++repeat) {// repeat 改成了1次
        printf("s=%d t=%d  repeat=%d\n",s,t,repeat);


        


        uint64_t gen_ns = real_time();
        uint64_t run_res = q4112_run(inner_keys, inner_vals, inner_tuples[s],
                                     outer_join_keys, outer_aggr_keys, outer_vals, outer_tuples[s], threads[t]);
        gen_ns = real_time() - gen_ns;
        printf("gen_ns %ld\n", gen_ns);

        // printf("gen_res : %llu\n", (unsigned long long) gen_res);
        // printf("run_res : %llu\n", (unsigned long long) run_res);
        assert(gen_res == run_res);

        fprintf(fd, "%ld,%f,%d,%ld,%f,%d,%ld,%ld,%f,%d,%d,%ld\n",inner_tuples[s], inner_selectivity[s], inner_val_max[s],
                outer_tuples[s], outer_selectivity[s], outer_val_max[s],
                groups[s], hh_groups[s], hh_probability[s],
                threads[t], repeat, gen_ns);

        
        //        gen_ns = real_time() - gen_ns;
      }
    }

    free(inner_keys);
    free(inner_vals); 
    free(outer_join_keys);
    free(outer_aggr_keys);
    free(outer_vals);
  }
  
  fclose(fd);
  return EXIT_SUCCESS;
}
  




