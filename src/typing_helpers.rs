pub fn parse_int(s: &str) -> Option<i64> {
    s.trim().parse().ok()
}

pub fn parse_float(s: &str) -> Option<f64> {
    let l = s.trim().to_lowercase();
    if l == "nan" {
        Some(f64::NAN)
    } else {
        l.parse().ok()
    }
}

pub fn parse_bool(s: &str) -> Option<bool> {
    let l = s.trim().to_lowercase();
    if l == "1" || l == "t" {
        Some(true)
    } else if l == "0" || l == "f" {
        Some(false)
    } else {
        l.parse().ok()
    }
}

pub fn is_missing(s: &str) -> bool {
    s.trim().is_empty()
}
