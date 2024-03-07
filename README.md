# 1BRC Rust
See [1brc.dev](https://1brc.dev) for the challenge.

Feedback on this is welcome.

Decided to give it a shot and stick to the Rust std lib (no Rayon / Crossbeam for implementing map-reduce! 😅).

Browse the commits to watch me go from what I felt like was a reasonably naive solution which ran in 90-100 seconds
to the current solution which runs in about <8.5 seconds on my laptop (11th gen core i7 with 2x32GB DDR4).

This solution is not the end all be all.

### Solution summary:
(In case you don't want to browse the commits / read `src/main.rs`)
1. Memory map the input file to a byte slice (`&[u8]`).
2. Chunk the byte slice into `NUM_THREADS` chunks (in my case this was 8),
   padding each chunk to the nearest `'\n'` delimiter.
3. Spawn `NUM_THREADS` threads, with each thread building a local hash map of
   `station name, (min/sum/max/count)` <- all stored as i32 (see `utf8_funky_int`).
4. Collect and merge each thread's hash map (the merge function could likely be improved
   but it's far from the hottest function).
5. Create a vector of the merged hash map's keys and sort it.
6. Output the sorted results (and convert all the `i32`'s to `f32`'s).

### Notes:
Although I say I stick to the std lib, I did do a rough implementation of the FNV hash algorithm that was mostly
based on `servo/fnv`'s crate. Additionally, I use `libc::mmap`, which, I consider to also be fair game.

### Output (truncated):
```
$ time ./target/release/onebrc-rs > /dev/null

real	0m8.026s
user	1m1.719s
sys	0m0.827s
```

### Hotspots:
```
Function                                           Module     CPU Time  % of CPU Time(%)
-------------------------------------------------  ---------  --------  ----------------
std::...::__rust_begin_short_backtrace::...        onebrc-rs   38.036s             60.5%   (the mapper fn)
hashbrown::..::rustc_entry::...                    onebrc-rs   16.742s             26.6%   (from updating ea thread's hashmap)
__memcmp_evex_movbe                                libc.so.6    8.042s             12.8%   (from mmap)
```
Further customizing the hashmap / hashing functionality is of interest. I think ultimately using a lookup table based mechanism could be beneficial.

### Utilization:
Nearly 50% cpu bound, 40% memory bound (again, on laptop). Poor branch speculation is a lingering issue.
Would also be interested to possibly improve physical core affinity for each thread.
