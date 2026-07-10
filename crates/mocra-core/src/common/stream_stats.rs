use crate::cacheable::CacheAble;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
/// is close or not
pub struct StreamStats(pub bool);
impl CacheAble for StreamStats {
    fn field() -> impl AsRef<str> {
        "stream_stats"
    }
}
