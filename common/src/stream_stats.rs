use serde::{Deserialize, Serialize};
use cacheable::CacheAble;
#[derive(Serialize, Deserialize)]
/// is close or not
pub struct StreamStats(pub bool);
impl CacheAble for StreamStats {
    fn field() -> impl AsRef<str> {
        "stream_stats"
    }
}