use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const THRESHOLD: usize = 2;
const COOLDOWN: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Circuit {
    generation: u64,
    failures: usize,
    blocked_until: Option<Instant>,
    probing: bool,
}

pub struct Backend {
    pub name: &'static str,
    pub url: String,
    circuit: Mutex<Circuit>,
}

impl Backend {
    pub fn new(name: &'static str, url: String) -> Self {
        Self {
            name,
            url,
            circuit: Mutex::default(),
        }
    }

    pub fn status(&self) -> (&'static str, usize) {
        let c = self.circuit.lock().unwrap();
        let state = match c.blocked_until {
            None => "closed",
            Some(t) if t > Instant::now() => "open",
            Some(_) if c.probing => "probing",
            Some(_) => "probe-ready",
        };
        (state, c.failures)
    }

    pub fn acquire(self: &Arc<Self>) -> Option<Attempt> {
        let mut c = self.circuit.lock().unwrap();
        if let Some(t) = c.blocked_until {
            if t > Instant::now() || c.probing {
                return None;
            }
            c.probing = true;
        }
        Some(Attempt {
            backend: self.clone(),
            generation: c.generation,
            finished: false,
        })
    }
}

pub struct Attempt {
    backend: Arc<Backend>,
    generation: u64,
    finished: bool,
}

impl Attempt {
    pub fn finish(mut self, healthy: bool) {
        self.finished = true;
        let mut c = self.backend.circuit.lock().unwrap();
        // Ignore responses from requests admitted before a later circuit transition.
        if c.generation != self.generation {
            return;
        }
        if healthy {
            c.failures = 0;
            if c.blocked_until.take().is_some() {
                c.generation += 1;
            }
            c.probing = false;
        } else {
            c.failures += 1;
            if c.failures >= THRESHOLD {
                c.generation += 1;
                c.blocked_until = Some(Instant::now() + COOLDOWN);
                c.probing = false;
            }
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut c = self.backend.circuit.lock().unwrap();
        if c.generation == self.generation && c.probing {
            c.probing = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn late_success_cannot_close_open_circuit_and_only_one_probe_runs() {
        let b = Arc::new(Backend::new("A", String::new()));
        let stale = b.acquire().unwrap();
        for _ in 0..2 {
            b.acquire().unwrap().finish(false);
        }
        stale.finish(true);
        assert_eq!(b.status(), ("open", 2));
        assert!(b.acquire().is_none());
        b.circuit.lock().unwrap().blocked_until = Some(Instant::now());
        let cancelled = b.acquire().unwrap();
        assert!(b.acquire().is_none());
        drop(cancelled);
        b.acquire().unwrap().finish(true);
        assert_eq!(b.status(), ("closed", 0));
    }
}
