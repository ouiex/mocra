use std::sync::Arc;
use std::time::Instant;

trait Module {
    fn name(&self) -> &str;
}

struct Impl {
    n: String,
}

impl Module for Impl {
    fn name(&self) -> &str { &self.n }
}

fn bench_box_move_call(iter: usize) -> u128 {
    let m = Box::new(Impl { n: "m".into() }) as Box<dyn Module>;
    let start = Instant::now();
    // move into local scope repeatedly by recreating (simulate move cost)
    for _ in 0..iter {
        let b = &m;
        let _ = b.name();
    }
    start.elapsed().as_nanos()
}

fn bench_deep_clone_box(iter: usize) -> u128 {
    // simulate deep clone by cloning inner string many times
    let m0 = Impl { n: "m".repeat(100) };
    let start = Instant::now();
    for _ in 0..iter {
        let _clone = Impl { n: m0.n.clone() }; // expensive deep clone
        let _ = _clone.name();
    }
    start.elapsed().as_nanos()
}

fn bench_arc_clone_call(iter: usize) -> u128 {
    let a = Arc::new(Impl { n: "m".into() }) as Arc<dyn Module + Send + Sync>;
    let start = Instant::now();
    for _ in 0..iter {
        let c = Arc::clone(&a); // atomic inc
        let _ = c.name();
        // drop c here (atomic dec)
    }
    start.elapsed().as_nanos()
}

fn bench_vtable_call(iter: usize) -> u128 {
    let a = Arc::new(Impl { n: "m".into() }) as Arc<dyn Module + Send + Sync>;
    let start = Instant::now();
    for _ in 0..iter {
        let _ = a.name(); // vtable dispatch
    }
    start.elapsed().as_nanos()
}

fn main() {
    let iter = 5_000_000usize;
    println!("box move+call ns: {}", bench_box_move_call(iter));
    println!("box deep-clone+call ns: {}", bench_deep_clone_box(100_000));
    println!("arc clone+call ns: {}", bench_arc_clone_call(iter));
    println!("vtable call ns: {}", bench_vtable_call(iter));
}