use crate::Storage;
use crate::error::{Result, StorageError};
use wind_wire::event::{FeedEvent, Scope, Source, Target};

impl Storage {
    /// 分配本次 feed 运行的 epoch
    pub async fn allocate_feed_epoch(&self) -> Result<i64> {
        let epoch = self
            .executor
            .fetch_one_scalar(sqlx::query_scalar::<_, i64>(
                "UPDATE feed_meta SET epoch = epoch + 1
                  WHERE singleton = 1
                  RETURNING epoch",
            ))
            .await?;
        Ok(epoch)
    }

    /// 整批写入事件日志
    pub async fn append_feed_batch(&self, events: &[FeedEvent]) -> Result<()> {
        self.with_tx(async move |tx| {
            for event in events {
                tx.append_feed_event(event).await?;
            }
            Ok(())
        })
        .await
    }

    async fn append_feed_event(&self, event: &FeedEvent) -> Result<()> {
        let envelope = &event.envelope;
        let (topic_id, instance_id) = match &envelope.scope {
            Scope::Global => (None, None),
            Scope::Topic {
                topic_id,
                instance_id,
            } => (Some(*topic_id), Some(*instance_id)),
        };
        let (source_kind, source_name, source_version) = match &envelope.source {
            Source::Core { subsystem } => (envelope.source.as_ref(), subsystem.as_str(), 0_i64),
            Source::Mod { name, version } => {
                (envelope.source.as_ref(), name.as_str(), i64::from(*version))
            }
        };
        let (route_kind, route_name, route_channel) = match &event.target {
            Target::Core => ("core", None, None),
            Target::Mod { name, channel } => ("mod", Some(name.as_str()), Some(channel.as_str())),
        };
        let payload = envelope
            .payload
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        let inserted = self
            .executor
            .fetch_optional(
                sqlx::query_as::<_, (i64,)>(
                    "INSERT INTO event_log
                        (epoch, seq, create_time, name, shape, route_kind, route_name,
                         route_channel, topic_id, instance_id, origin, source_kind, source_name,
                         source_version, payload)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (epoch, seq) DO NOTHING
                     RETURNING id",
                )
                .bind(event.key.epoch)
                .bind(event.key.seq)
                .bind(envelope.time)
                .bind(envelope.name.as_str())
                .bind(envelope.shape.as_ref())
                .bind(route_kind)
                .bind(route_name)
                .bind(route_channel)
                .bind(topic_id)
                .bind(instance_id)
                .bind(envelope.origin.as_ref())
                .bind(source_kind)
                .bind(source_name)
                .bind(source_version)
                .bind(payload.as_deref()),
            )
            .await?;
        if inserted.is_some() {
            return Ok(());
        }
        Err(StorageError::IdentityConflict {
            epoch: event.key.epoch,
            seq: event.key.seq,
        })
    }
}
