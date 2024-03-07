// Read data. Calculate min/ave/max. Print results alphabetically by station name.
use libc::{mmap, off_t, MAP_PRIVATE, PROT_READ};
use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::Error;
use std::os::fd::AsRawFd;
use std::ptr::null_mut;
use std::str::{from_utf8, from_utf8_unchecked};
use std::thread::available_parallelism;
use std::{slice, thread};

const WEATHER_DATA: &str = "measurements.txt";

#[derive(Clone)]
struct WeatherEntry {
    min: i32,
    sum: i32,
    max: i32,
    cnt: i32, // Make it easier to divide ave/cnt.
}

impl WeatherEntry {
    fn update(&mut self, temp: i32) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.cnt += 1;
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
            min: i32::MAX,
            sum: 0,
            max: i32::MIN,
            cnt: 0,
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

// Get indices [a,b) into a byte slice to get a chunk of size n_1 = b - a where
// n_1 >= n based on the next instance of the given delimiter.
#[inline]
fn chunker(start: usize, n: usize, delim: u8, b: &[u8]) -> (usize, usize) {
    let end = (start + n).min(b.len());
    let pad = b[end..].iter().position(|c| c == &delim).unwrap_or(0);
    (start, end + pad)
}

// Parse a utf8 encoded byte slice representing a floating point number (that we know in advance
// will have precision to the tens place at most) as an integer.
#[inline]
fn utf8_funky_int(b: &[u8]) -> Result<i32, ()> {
    let sign = if let Some(sign) = b.first() {
        sign == &b'-'
    } else {
        return Err(());
    };
    // SAFETY: Inputs are known to be valid UTF8, however improper map-reduce logic could yield
    // an unforeseen edge case that could cause a panic. Beware.
    let int = unsafe { from_utf8_unchecked(b) }.chars().fold(0, |acc, c| {
        if let Some(d) = c.to_digit(10) {
            acc * 10 + d
        } else {
            acc
        }
    });

    if sign {
        Ok(-(int as i32))
    } else {
        Ok(int as i32)
    }
}

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
    let thirds = (end - start) / 3;
    let (start1, end1) = chunker(start, thirds, b'\n', mmap_bytes);
    let (start2, end2) = chunker(end1 + 1, thirds, b'\n', mmap_bytes);
    let (start3, end3) = chunker(end2 + 1, thirds, b'\n', mmap_bytes);
    let mut lines1 = mmap_bytes[start1..end1].split(|c| c == &b'\n');
    let mut lines2 = mmap_bytes[start2..end2].split(|c| c == &b'\n');
    let mut lines3 = mmap_bytes[start3..end3].split(|c| c == &b'\n');
    let mut map: LilFnvHashMap<&[u8], WeatherEntry> = LilFnvHashMap::default();
    loop {
        if (&mut lines1).peekable().peek().is_none() {
            break;
        }
        if (&mut lines2).peekable().peek().is_none() {
            break;
        }
        if (&mut lines3).peekable().peek().is_none() {
            break;
        }
        let line1 = match lines1.next() {
            Some(l) => l,
            None => break,
        };
        let line2 = match lines2.next() {
            Some(l) => l,
            None => break,
        };
        let line3 = match lines3.next() {
            Some(l) => l,
            None => break,
        };
        let mut delim1 = line1.split(|c| c == &b';');
        let mut delim2 = line2.split(|c| c == &b';');
        let mut delim3 = line3.split(|c| c == &b';');
        let name1 = delim1.next().unwrap();
        let name2 = delim2.next().unwrap();
        let name3 = delim3.next().unwrap();
        let temp1 = utf8_funky_int(delim1.next().unwrap()).unwrap();
        let temp2 = utf8_funky_int(delim2.next().unwrap()).unwrap();
        let temp3 = utf8_funky_int(delim3.next().unwrap()).unwrap();
        let entry = map.entry(name1).or_default();
        (*entry).update(temp1);
        let entry = map.entry(name2).or_default();
        (*entry).update(temp2);
        let entry = map.entry(name3).or_default();
        (*entry).update(temp3);
    }

    for line in lines1 {
        let mut delim = line.split(|c| c == &b';');
        let name = delim.next().unwrap();
        let temp = utf8_funky_int(delim.next().unwrap()).unwrap();
        let entry = map.entry(name).or_default();
        (*entry).update(temp);
    }

    for line in lines2 {
        let mut delim = line.split(|c| c == &b';');
        let name = delim.next().unwrap();
        let temp = utf8_funky_int(delim.next().unwrap()).unwrap();
        let entry = map.entry(name).or_default();
        (*entry).update(temp);
    }

    for line in lines3 {
        let mut delim = line.split(|c| c == &b';');
        let name = delim.next().unwrap();
        let temp = utf8_funky_int(delim.next().unwrap()).unwrap();
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
    let mmap_size = mmap_size - 1; // TODO: hack to avoid getting last '\n' in a chunk.
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
        println!(
            "{};{};{};{}",
            name,
            (val.min as f32) / 10.0,
            (val.sum as f32) / ((val.cnt * 10) as f32),
            (val.max as f32) / 10.0 //val.min,
                                    //val.sum / val.cnt,
                                    //val.max
        );
    }

    Ok(())
}
