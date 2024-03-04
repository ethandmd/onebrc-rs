// Read data. Calculate min/ave/max. Print results alphabetically by station name.
use std::collections::HashMap;
use std::fs::read_to_string;

const WEATHER_DATA: &str = "measurements.txt";

struct WeatherEntry {
    min: f64,
    ave: f64,
    max: f64,
    cnt: f64, // Make it easier to divide ave/cnt.
}

impl WeatherEntry {
    fn new(min: f64, ave: f64, max: f64) -> Self {
        Self {
            min,
            ave,
            max,
            cnt: 0.0,
        }
    }
}

fn main() {
    let mut map: HashMap<&str, WeatherEntry> = HashMap::new();
    let data = read_to_string(WEATHER_DATA).expect("Weather data not found.");
    for line in data.split('\n') {
        if line == "" {
            continue;
        }
        let line: Vec<&str> = line.split(';').collect();
        let station = line[0];
        let temp: f64 = line[1].parse().expect("Unable to parse float from string.");
        if let Some(val) = map.get_mut(station) {
            if temp < val.min {
                val.min = temp;
            }
            if temp > val.max {
                val.max = temp;
            }
            val.cnt += 1.0;
            val.ave = (val.ave + temp) / val.cnt;
        } else {
            map.insert(station, WeatherEntry::new(temp, temp, temp));
        }
    }
    let mut keys: Vec<&&str> = map.keys().collect();
    keys.sort_by(|a, b| a.partial_cmp(b).expect("Sort by cmp fn didn't work."));
    for key in keys {
        let val = map
            .get(key)
            .expect("Couldn't find key that should be there.");
        println!("{};{};{};{}", key, val.min, val.ave, val.max);
    }
}
