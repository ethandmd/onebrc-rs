// Read data. Calculate min/ave/max. Print results alphabetically by station name.
use libc::{c_void, mmap, off_t, MAP_PRIVATE, PROT_READ};
use std::collections::HashMap;
use std::fs::File;
use std::io::Error;
use std::os::fd::AsRawFd;
use std::ptr::null_mut;
use std::thread::available_parallelism;
use std::{slice, thread};

const WEATHER_DATA: &'static str = "measurements.txt";

#[derive(Clone)]
struct WeatherEntry {
    min: f64,
    sum: f64,
    max: f64,
    cnt: f64, // Make it easier to divide ave/cnt.
}

impl WeatherEntry {
    fn new(temp: f64) -> Self {
        Self {
            min: temp,
            sum: temp,
            max: temp,
            cnt: 1.0,
        }
    }

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

fn merge<'a>(
    mut left: HashMap<&'a str, WeatherEntry>,
    right: HashMap<&'a str, WeatherEntry>,
) -> HashMap<&'a str, WeatherEntry> {
    for (right_key, right_val) in right.into_iter() {
        if let Some(left_val) = left.get_mut(right_key) {
            left_val.merge(&right_val);
        } else {
            left.insert(&right_key, right_val.to_owned());
        }
    }
    left
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
            null_mut() as *mut c_void,
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
        let chunk_end = end + pad;
        #[cfg(debug_assertions)]
        println!("Starting thread {} at bytes[{}..{}]", _id, start, chunk_end);
        //thread::scope(|s| {
        let t = thread::Builder::new().name(_id.to_string());
        handles.push(
            t.spawn(move || {
                let mut map: HashMap<&str, WeatherEntry> = HashMap::new();
                for line in mmap_bytes[start..chunk_end].split(|c| c == &b'\n') {
                    let mut s = std::str::from_utf8(line).unwrap().split(';');
                    if line.is_empty() {
                        continue;
                    }
                    let name = s.next().unwrap();
                    let temp = s.next().unwrap().parse::<f64>().unwrap();
                    if let Some(entry) = map.get_mut(name) {
                        entry.update(temp);
                    } else {
                        map.insert(name, WeatherEntry::new(temp));
                    }
                }
                map
            })
            .unwrap(),
        );
        //});
        start += chunk_end - start + 1;
    }

    let report = handles
        .into_iter()
        .map(|t| t.join().unwrap())
        .reduce(|left, right| merge(left, right))
        .unwrap();

    let mut sorts: Vec<&&str> = report.keys().collect();
    sorts.sort_by(|a, b| a.partial_cmp(b).expect("Sort by cmp fn didn't work."));
    for name in sorts {
        let val = report.get(name).unwrap();
        println!("{};{};{};{}", name, val.min, val.sum / val.cnt, val.max);
    }

    //unsafe {
    //    munmap(m, mmap_size);
    //}

    Ok(())
}
