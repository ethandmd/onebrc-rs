// Read data. Calculate min/ave/max. Print results alphabetically by station name.
use libc::{mmap, off_t, MAP_PRIVATE, PROT_READ};
use std::collections::HashMap;
//use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::Error;
use std::os::fd::AsRawFd;
use std::ptr::null_mut;
use std::str::from_utf8;
use std::thread::available_parallelism;
use std::{slice, thread};

const WEATHER_DATA: &str = "measurements.txt";

#[derive(Clone)]
struct WeatherEntry {
    min: f64,
    sum: f64,
    max: f64,
    cnt: f64, // Make it easier to divide ave/cnt.
}

impl WeatherEntry {
    fn update(&mut self, temp: f64) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.cnt += 1.0;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.cnt += other.cnt;
    }
}

impl Default for WeatherEntry {
    fn default() -> Self {
        Self {
            min: f64::MAX,
            sum: 0.0,
            max: f64::MIN,
            cnt: 0.0,
        }
    }
}

#[derive(Clone)]
struct LilFnvHasher(u64);

const INITIAL_STATE: u64 = 0xcbf2_9ce4_8422_2325;
const PRIME: u64 = 0x0100_0000_01b3;

impl Default for LilFnvHasher {
    #[inline]
    fn default() -> Self {
        LilFnvHasher(INITIAL_STATE)
    }
}

impl Hasher for LilFnvHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes.iter() {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

type LilFnvHashBuilder = BuildHasherDefault<LilFnvHasher>;
type LilFnvHashMap<K, V> = HashMap<K, V, LilFnvHashBuilder>;

#[inline]
fn merge<'a>(
    mut left: LilFnvHashMap<&'a [u8], WeatherEntry>,
    right: LilFnvHashMap<&'a [u8], WeatherEntry>,
) -> LilFnvHashMap<&'a [u8], WeatherEntry> {
    for (right_key, right_val) in right.into_iter() {
        if let Some(left_val) = left.get_mut(right_key) {
            left_val.merge(&right_val);
        } else {
            left.insert(right_key, right_val.to_owned());
        }
    }
    left
}

#[inline]
fn mapper(start: usize, end: usize, mmap_bytes: &[u8]) -> LilFnvHashMap<&[u8], WeatherEntry> {
    let mut map: LilFnvHashMap<&[u8], WeatherEntry> = LilFnvHashMap::default();
    for line in mmap_bytes[start..end].split(|c| c == &b'\n') {
        if line.is_empty() {
            continue;
        }
        let mut delim = line.split(|c| c == &b';');
        //let name = std::str::from_utf8(delim.next().unwrap()).unwrap();
        let name = delim.next().unwrap();
        // SAFETY: We already know the inputs are valid utf8.
        let tmp = delim.next().unwrap();
        let temp = unsafe { std::str::from_utf8_unchecked(tmp) }
            .parse::<f64>()
            .unwrap();
        let entry = map.entry(name).or_default();
        (*entry).update(temp);
    }
    map
}

fn main() -> std::io::Result<()> {
    let num_threads = available_parallelism().unwrap().into();
    #[cfg(debug_assertions)]
    println!("Using {} threads.", num_threads);

    let f = File::open(WEATHER_DATA)?;
    let mmap_size = f.metadata().unwrap().len() as usize;
    #[cfg(debug_assertions)]
    println!("Data size: {}", mmap_size);
    let chunk_size = mmap_size / num_threads;
    #[cfg(debug_assertions)]
    println!("Estimated chunk size: {}", chunk_size);

    let m = unsafe {
        mmap(
            null_mut(),
            mmap_size,
            PROT_READ,
            MAP_PRIVATE,
            f.as_raw_fd(),
            0 as off_t,
        )
    };

    if m.is_null() {
        return Err(Error::from_raw_os_error(-1));
    }

    let bytes_ptr: *mut u8 = m.cast(); // Take ownership of the mmap ptr so we don't manually drop.
    let mmap_bytes = unsafe { slice::from_raw_parts(bytes_ptr, mmap_size) };
    let mut start = 0;
    let mut handles = Vec::new();
    for _id in 0..num_threads {
        let end = (start + chunk_size).min(mmap_size);
        let pad = mmap_bytes[end..]
            .iter()
            .position(|c| c == &b'\n')
            .unwrap_or(0);
        let end = end + pad;
        #[cfg(debug_assertions)]
        println!("Starting thread {} at bytes[{}..{}]", _id, start, end);
        //thread::scope(|s| {
        let t = thread::Builder::new().name(_id.to_string());
        handles.push(t.spawn(move || mapper(start, end, mmap_bytes)).unwrap());
        start += end - start + 1;
    }

    let report = handles
        .into_iter()
        .map(|t| t.join().unwrap())
        .reduce(|left, right| merge(left, right))
        .unwrap();

    let mut sorts: Vec<&str> = report.keys().map(|b| from_utf8(b).unwrap()).collect();
    sorts.sort_by(|a, b| a.partial_cmp(b).expect("Sort by cmp fn didn't work."));
    for name in sorts {
        let val = report.get(name.as_bytes()).unwrap();
        println!("{};{};{};{}", name, val.min, val.sum / val.cnt, val.max);
    }

    Ok(())
}
