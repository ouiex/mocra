use crate::common::model::config::Config;
use crate::common::model::{PayloadCodec, QueueEnvelope};
use once_cell::sync::OnceCell;
use rmp_serde as rmps;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_path_to_error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueCodec {
    Json,
    Msgpack,
}

impl QueueCodec {
    fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "json" => Self::Json,
            "msgpack" | "rmp" => Self::Msgpack,
            _ => Self::Msgpack,
        }
    }

    fn payload_codec(self) -> PayloadCodec {
        match self {
            Self::Json => PayloadCodec::Json,
            Self::Msgpack => PayloadCodec::MsgPack,
        }
    }

    fn from_payload_codec(codec: PayloadCodec) -> Self {
        match codec {
            PayloadCodec::Json => Self::Json,
            PayloadCodec::MsgPack => Self::Msgpack,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueMessageCodec {
    codec: QueueCodec,
}

impl Default for QueueMessageCodec {
    fn default() -> Self {
        Self {
            codec: QueueCodec::Msgpack,
        }
    }
}

impl QueueMessageCodec {
    pub(crate) fn from_config(cfg: &Config) -> Self {
        Self {
            codec: QueueCodec::from_name(
                cfg.channel_config
                    .queue_codec
                    .as_deref()
                    .unwrap_or("msgpack"),
            ),
        }
    }

    pub(crate) fn from_name(name: &str) -> Self {
        Self {
            codec: QueueCodec::from_name(name),
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self.codec {
            QueueCodec::Json => "json",
            QueueCodec::Msgpack => "msgpack",
        }
    }

    pub(crate) fn payload_codec(self) -> PayloadCodec {
        self.codec.payload_codec()
    }

    pub(crate) fn encode<T: Serialize>(self, value: &T) -> crate::errors::Result<Vec<u8>> {
        match self.codec {
            QueueCodec::Json => serde_json::to_vec(value)
                .map_err(|e| crate::errors::Error::new(crate::errors::ErrorKind::Queue, Some(e))),
            QueueCodec::Msgpack => rmps::to_vec(value)
                .map_err(|e| crate::errors::Error::new(crate::errors::ErrorKind::Queue, Some(e))),
        }
    }

    pub(crate) fn decode<T: DeserializeOwned>(self, bytes: &[u8]) -> crate::errors::Result<T> {
        match self.codec {
            QueueCodec::Json => serde_json::from_slice::<T>(bytes)
                .map_err(|e| crate::errors::Error::new(crate::errors::ErrorKind::Queue, Some(e))),
            QueueCodec::Msgpack => {
                let mut deserializer = rmps::Deserializer::new(bytes);
                serde_path_to_error::deserialize(&mut deserializer).map_err(|e| {
                    crate::errors::Error::new(crate::errors::ErrorKind::Queue, Some(e))
                })
            }
        }
    }

    pub(crate) fn encode_envelope<T: Serialize>(
        self,
        schema_id: impl Into<String>,
        schema_version: u16,
        value: &T,
    ) -> crate::errors::Result<QueueEnvelope> {
        let payload = self.encode(value)?;
        QueueEnvelope::new(schema_id, schema_version, self.codec.payload_codec(), payload)
            .map_err(|e| crate::errors::Error::new(crate::errors::ErrorKind::Queue, Some(e)))
    }

    pub(crate) fn decode_envelope<T: DeserializeOwned>(
        self,
        envelope: &QueueEnvelope,
    ) -> crate::errors::Result<T> {
        QueueMessageCodec {
            codec: QueueCodec::from_payload_codec(envelope.codec),
        }
        .decode(&envelope.payload)
    }
}

static QUEUE_CODEC_OVERRIDE: OnceCell<QueueMessageCodec> = OnceCell::new();

pub(crate) fn queue_codec() -> QueueMessageCodec {
    QUEUE_CODEC_OVERRIDE.get().copied().unwrap_or_default()
}

pub(crate) fn set_queue_codec_from_config(cfg: &Config) {
    let _ = QUEUE_CODEC_OVERRIDE.set(QueueMessageCodec::from_config(cfg));
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::common::model::{PayloadCodec, TypedEnvelope};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct SamplePayload {
        id: u32,
        name: String,
    }

    #[test]
    fn queue_message_codec_roundtrips_json_and_msgpack() {
        let sample = SamplePayload {
            id: 7,
            name: "demo".to_string(),
        };

        let json_codec = QueueMessageCodec::from_name("json");
        let json_bytes = json_codec.encode(&sample).expect("json encode should work");
        let json_roundtrip: SamplePayload =
            json_codec.decode(&json_bytes).expect("json decode should work");
        assert_eq!(json_roundtrip, sample);

        let msgpack_codec = QueueMessageCodec::from_name("msgpack");
        let msgpack_bytes = msgpack_codec
            .encode(&sample)
            .expect("msgpack encode should work");
        let msgpack_roundtrip: SamplePayload = msgpack_codec
            .decode(&msgpack_bytes)
            .expect("msgpack decode should work");
        assert_eq!(msgpack_roundtrip, sample);
    }

    #[test]
    fn queue_message_codec_builds_and_decodes_queue_envelope() {
        let codec = QueueMessageCodec::from_name("msgpack");
        let payload = SamplePayload {
            id: 42,
            name: "transport".to_string(),
        };

        let envelope = codec
            .encode_envelope("task.dispatch", 1, &payload)
            .expect("queue envelope should build");
        assert_eq!(envelope.schema_id, "task.dispatch");
        assert_eq!(envelope.schema_version, 1);
        assert_eq!(envelope.codec, PayloadCodec::MsgPack);

        let decoded: SamplePayload = codec
            .decode_envelope(&envelope)
            .expect("queue envelope should decode");
        assert_eq!(decoded, payload);

        let typed = TypedEnvelope::new(
            envelope.schema_id.clone(),
            envelope.schema_version,
            envelope.codec,
            envelope.payload.clone(),
        );
        assert_eq!(QueueEnvelope::from_typed(typed).unwrap(), envelope);
    }
}
