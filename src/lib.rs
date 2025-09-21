use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Switflake ID bit layout
/// - 39 bits: timestamp (단위: 10ms)
/// - 8 bits: sequence
/// - 16 bits: machine_id
/// - 1 bit: reserved (always 0)
const TIMESTAMP_BITS: u64 = 39;
const SEQUENCE_BITS: u64 = 8;
const MACHINE_ID_BITS: u64 = 16;

const TIMESTAMP_SHIFT: u64 = SEQUENCE_BITS + MACHINE_ID_BITS;
const SEQUENCE_SHIFT: u64 = MACHINE_ID_BITS;

const TIMESTAMP_MASK: u64 = (1 << TIMESTAMP_BITS) - 1;
const SEQUENCE_MASK: u64 = (1 << SEQUENCE_BITS) - 1;
const MACHINE_ID_MASK: u64 = (1 << MACHINE_ID_BITS) - 1;

/// Switflake epoch: 2021-01-01 00:00:00 UTC
const DEFAULT_EPOCH: u64 = 1609459200000;

/// Ring buffer 크기 (bucket mode)
const DEFAULT_BUCKET_SIZE: usize = 10000;

#[derive(Debug, Clone)]
pub struct SwitflakeError {
    msg: String,
}

impl std::fmt::Display for SwitflakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for SwitflakeError {}

/// ID 생성 모드
#[derive(Debug, Clone, Copy)]
pub enum GeneratorMode {
    /// CAS
    Realtime,
    /// Ring buffer
    Bucket,
    /// Bulk iterate
    Lazy,
}

/// Switflake ID 생성기 settings
/// TODO: Setting이 아니라 builder and default로
#[derive(Debug, Clone)]
pub struct Settings {
    pub epoch: u64,
    pub machine_id: u16,
    pub mode: GeneratorMode,
    pub bucket_size: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            epoch: DEFAULT_EPOCH,
            machine_id: 0,
            mode: GeneratorMode::Realtime,
            bucket_size: DEFAULT_BUCKET_SIZE,
        }
    }
}

impl Settings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn with_machine_id(mut self, id: u16) -> Self {
        self.machine_id = id;
        self
    }

    pub fn with_mode(mut self, mode: GeneratorMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_bucket_size(mut self, size: usize) -> Self {
        self.bucket_size = size;
        self
    }
}

/// CAS based id generators
#[derive(Clone)]
pub struct Switflake {
    inner: Arc<SwitflakeInner>,
}

struct SwitflakeInner {
    epoch: u64,
    machine_id: u16,
    mode: GeneratorMode,
    // CAS 기반 상태 관리 (timestamp << 8 | sequence)
    state: AtomicU64,
    // Bucket mode 전용
    bucket: Option<Arc<Mutex<IdBucket>>>,
    // Lazy mode 전용
    lazy_cache: Option<Arc<Mutex<Vec<u64>>>>,
}

/// Ring buffer based id bucket
struct IdBucket {
    ids: VecDeque<u64>,
    generator: Box<dyn IdGenerator + Send>,
    capacity: usize,
}

trait IdGenerator {
    fn generate(&mut self) -> Result<u64, SwitflakeError>;
}

struct RealtimeGenerator {
    epoch: u64,
    machine_id: u16,
    last_timestamp: u64,
    sequence: u16,
}

impl Switflake {
    pub fn new(settings: Settings) -> Result<Self, SwitflakeError> {
        let inner = match settings.mode {
            GeneratorMode::Realtime => SwitflakeInner {
                epoch: settings.epoch,
                machine_id: settings.machine_id,
                mode: settings.mode,
                state: AtomicU64::new(0),
                bucket: None,
                lazy_cache: None,
            },
            GeneratorMode::Bucket => {
                let generator = RealtimeGenerator {
                    epoch: settings.epoch,
                    machine_id: settings.machine_id,
                    last_timestamp: 0,
                    sequence: 0,
                };

                let mut bucket = IdBucket {
                    ids: VecDeque::with_capacity(settings.bucket_size),
                    generator: Box::new(generator),
                    capacity: settings.bucket_size,
                };

                // Fill the bucket with initial IDs
                bucket.fill()?;

                SwitflakeInner {
                    epoch: settings.epoch,
                    machine_id: settings.machine_id,
                    mode: settings.mode,
                    state: AtomicU64::new(0),
                    bucket: Some(Arc::new(Mutex::new(bucket))),
                    lazy_cache: None,
                }
            }
            GeneratorMode::Lazy => SwitflakeInner {
                epoch: settings.epoch,
                machine_id: settings.machine_id,
                mode: settings.mode,
                state: AtomicU64::new(0),
                bucket: None,
                lazy_cache: Some(Arc::new(Mutex::new(Vec::with_capacity(100)))),
            },
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Generate the ID based on the current state
    pub fn next_id(&self) -> Result<u64, SwitflakeError> {
        match self.inner.mode {
            GeneratorMode::Realtime => self.next_id_realtime(),
            GeneratorMode::Bucket => self.next_id_bucket(),
            GeneratorMode::Lazy => self.next_id_lazy(),
        }
    }

    /// Generate the real-time ID based on CAS
    fn next_id_realtime(&self) -> Result<u64, SwitflakeError> {
        loop {
            let current_time = self.current_time();
            let old_state = self.inner.state.load(Ordering::Acquire);

            let old_timestamp = old_state >> 8;
            let old_sequence = (old_state & 0xFF) as u16;

            let (timestamp, sequence) = if current_time == old_timestamp {
                let new_seq = (old_sequence + 1) & (SEQUENCE_MASK as u16);
                if new_seq == 0 {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                (current_time, new_seq)
            } else if current_time > old_timestamp {
                (current_time, 0)
            } else {
                // TODO: 에러를 좀 묶기 술코딩 ㄴㄴ
                return Err(SwitflakeError {
                    msg: "Clock moved backwards".to_string(),
                });
            };

            let new_state = (timestamp << 8) | (sequence as u64);

            // CAS: exchange
            match self.inner.state.compare_exchange(
                old_state,
                new_state,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(self.make_id(timestamp, sequence, self.inner.machine_id));
                }
                Err(_) => continue, // 재시도
            }
        }
    }

    /// Bucket(ring buffer)
    fn next_id_bucket(&self) -> Result<u64, SwitflakeError> {
        if let Some(bucket) = &self.inner.bucket {
            let mut bucket = bucket.lock();
            if bucket.ids.is_empty() {
                bucket.fill()?;
            }

            bucket.ids.pop_front().ok_or_else(|| SwitflakeError {
                msg: "Bucket is empty".to_string(),
            })
        } else {
            //TODO: 에러좀 위로
            Err(SwitflakeError {
                msg: "Bucket not initialized".to_string(),
            })
        }
    }

    /// Lazy bulk
    fn next_id_lazy(&self) -> Result<u64, SwitflakeError> {
        if let Some(cache) = &self.inner.lazy_cache {
            let mut cache = cache.lock();

            if cache.is_empty() {
                for _ in 0..10 {
                    let id = self.next_id_realtime()?;
                    cache.push(id);
                }
            }

            cache.pop().ok_or_else(|| SwitflakeError {
                msg: "Cache is empty".to_string(),
            })
        } else {
            self.next_id_realtime()
        }
    }

    /// Current time (10ms)
    fn current_time(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        (now - self.inner.epoch) / 10
    }

    /// Construct an ID
    fn make_id(&self, timestamp: u64, sequence: u16, machine_id: u16) -> u64 {
        ((timestamp & TIMESTAMP_MASK) << TIMESTAMP_SHIFT)
            | ((sequence as u64 & SEQUENCE_MASK) << SEQUENCE_SHIFT)
            | (machine_id as u64 & MACHINE_ID_MASK)
    }

    /// Decompose an ID
    pub fn decompose(id: u64) -> DecomposedId {
        DecomposedId {
            timestamp: (id >> TIMESTAMP_SHIFT) & TIMESTAMP_MASK,
            sequence: ((id >> SEQUENCE_SHIFT) & SEQUENCE_MASK) as u16,
            machine_id: (id & MACHINE_ID_MASK) as u16,
        }
    }
}

impl IdBucket {
    fn fill(&mut self) -> Result<(), SwitflakeError> {
        let need = self.capacity - self.ids.len();
        for _ in 0..need {
            self.ids.push_back(self.generator.generate()?);
        }
        Ok(())
    }
}

impl IdGenerator for RealtimeGenerator {
    fn generate(&mut self) -> Result<u64, SwitflakeError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let timestamp = (now - self.epoch) / 10;

        if timestamp == self.last_timestamp {
            self.sequence = (self.sequence + 1) & (SEQUENCE_MASK as u16);
            if self.sequence == 0 {
                std::thread::sleep(Duration::from_millis(10));
                return self.generate();
            }
        } else {
            self.sequence = 0;
            self.last_timestamp = timestamp;
        }

        Ok(((timestamp & TIMESTAMP_MASK) << TIMESTAMP_SHIFT)
            | ((self.sequence as u64 & SEQUENCE_MASK) << SEQUENCE_SHIFT)
            | (self.machine_id as u64 & MACHINE_ID_MASK))
    }
}

/// Decomposed ID
/// TODO: 구조체가 아닌 명령어 단위로 전달할 방법 찾기
/// L1을 태울수 있게
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecomposedId {
    pub timestamp: u64,
    pub sequence: u16,
    pub machine_id: u16,
}

// 스레드 안전성 보장
unsafe impl Send for Switflake {}
unsafe impl Sync for Switflake {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::thread;

    #[test]
    fn test_realtime_generation() {
        let sf = Switflake::new(
            Settings::new()
                .with_machine_id(1)
                .with_mode(GeneratorMode::Realtime),
        )
        .unwrap();

        let mut ids = HashSet::new();
        for _ in 0..1000 {
            let id = sf.next_id().unwrap();
            assert!(ids.insert(id));
        }
    }

    #[test]
    fn test_bucket_generation() {
        let sf = Switflake::new(
            Settings::new()
                .with_machine_id(2)
                .with_mode(GeneratorMode::Bucket)
                .with_bucket_size(100),
        )
        .unwrap();

        let mut ids = HashSet::new();
        for _ in 0..200 {
            let id = sf.next_id().unwrap();
            assert!(ids.insert(id));
        }
    }

    #[test]
    fn test_lazy_generation() {
        let sf = Switflake::new(
            Settings::new()
                .with_machine_id(3)
                .with_mode(GeneratorMode::Lazy),
        )
        .unwrap();

        let mut ids = HashSet::new();
        for _ in 0..100 {
            let id = sf.next_id().unwrap();
            assert!(ids.insert(id));
        }
    }

    #[test]
    fn test_concurrent_generation() {
        let sf = Switflake::new(
            Settings::new()
                .with_machine_id(4)
                .with_mode(GeneratorMode::Realtime),
        )
        .unwrap();

        let mut handles = vec![];
        let ids = Arc::new(Mutex::new(HashSet::new()));

        for _ in 0..10 {
            let sf_clone = sf.clone();
            let ids_clone = ids.clone();

            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let id = sf_clone.next_id().unwrap();
                    ids_clone.lock().insert(id);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(ids.lock().len(), 1000);
    }

    #[test]
    fn test_decompose() {
        let sf = Switflake::new(Settings::new().with_machine_id(123)).unwrap();

        let id = sf.next_id().unwrap();
        let decomposed = Switflake::decompose(id);

        assert_eq!(decomposed.machine_id, 123);
    }

    #[test]
    fn test_ordering() {
        let sf = Switflake::new(
            Settings::new()
                .with_machine_id(5)
                .with_mode(GeneratorMode::Realtime),
        )
        .unwrap();

        let id1 = sf.next_id().unwrap();
        thread::sleep(Duration::from_millis(15));
        let id2 = sf.next_id().unwrap();

        assert!(id2 > id1);
    }
}
