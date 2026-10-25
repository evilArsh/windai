use super::Storage;
use super::TableName;
use super::utils::{de_str_to, ensure_lte_one, execute_allowing_zero_rows, now_ts, parse_str_to};
use crate::db::{DbDriver, DbRow};
use crate::delete_from;
use crate::error::Result;
use crate::insert;
use crate::models::{AgentPending, PendingCreate};
use crate::select_fields;
use sqlx::{FromRow, QueryBuilder, Row};

fn select_pending<'a>() -> QueryBuilder<'a, DbDriver> {
    select_fields!(
        TableName::AGENT_PENDING,
        (
            "id",
            "instance_id",
            "kind",
            "payload_json",
            "source_id",
            "created_at"
        )
    )
}

fn pending_insert_query<'a>(input: &PendingCreate) -> Result<QueryBuilder<'a, DbDriver>> {
    let payload_json = serde_json::to_string(&input.payload)?;
    Ok(insert!(
        TableName::AGENT_PENDING,
        ("instance_id", input.instance_id),
        ("kind", input.kind.as_ref().to_string()),
        ("payload_json", payload_json),
        ("source_id", input.source_id),
        ("created_at", now_ts()),
    ))
}

impl<'s> FromRow<'s, DbRow> for AgentPending {
    fn from_row(row: &'s DbRow) -> std::result::Result<Self, sqlx::Error> {
        let kind_text: String = row.try_get("kind")?;
        let payload_json: String = row.try_get("payload_json")?;
        Ok(Self {
            id: row.try_get("id")?,
            instance_id: row.try_get("instance_id")?,
            kind: parse_str_to(&kind_text).map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            payload: de_str_to(&payload_json)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
            source_id: row.try_get("source_id")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl Storage {
    /// 插入待处理记录并返回新 id
    pub async fn pending_insert(&self, input: PendingCreate) -> Result<i64> {
        let mut qb = pending_insert_query(&input)?;
        qb.push(" RETURNING id");
        self.executor
            .fetch_one_scalar(qb.build_query_scalar::<i64>())
            .await
    }

    /// 冲突时跳过插入，返回新 id 或 None
    pub async fn pending_insert_if_absent(&self, input: PendingCreate) -> Result<Option<i64>> {
        let mut qb = pending_insert_query(&input)?;
        qb.push(" ON CONFLICT DO NOTHING RETURNING id");
        let rows = self.executor.fetch_all_rows(qb.build()).await?;
        let ids = rows
            .into_iter()
            .map(|row| row.try_get("id"))
            .collect::<std::result::Result<Vec<i64>, sqlx::Error>>()?;
        ensure_lte_one(ids)
    }

    /// 列出实例的待处理记录，按 id 升序
    pub async fn pending_list_by_instance(&self, instance_id: i64) -> Result<Vec<AgentPending>> {
        self.executor
            .fetch_all(
                select_pending()
                    .push(" WHERE instance_id = ")
                    .push_bind(instance_id)
                    .push(" ORDER BY id ASC")
                    .build_query_as::<AgentPending>(),
            )
            .await
    }

    /// 按 id 取值，空 id 列表直接返回空
    pub async fn pending_list_by_ids(&self, ids: &[i64]) -> Result<Vec<AgentPending>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = select_pending();
        qb.push(" WHERE id IN (");
        {
            let mut separated = qb.separated(", ");
            for id in ids {
                separated.push_bind(*id);
            }
        }
        qb.push(") ORDER BY id ASC");
        self.executor
            .fetch_all(qb.build_query_as::<AgentPending>())
            .await
    }

    /// 按实例批量取值，先按实例再按 id 升序
    pub async fn pending_list_by_instances(
        &self,
        instance_ids: &[i64],
    ) -> Result<Vec<AgentPending>> {
        if instance_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = select_pending();
        qb.push(" WHERE instance_id IN (");
        {
            let mut separated = qb.separated(", ");
            for id in instance_ids {
                separated.push_bind(*id);
            }
        }
        qb.push(") ORDER BY instance_id ASC, id ASC");
        self.executor
            .fetch_all(qb.build_query_as::<AgentPending>())
            .await
    }

    /// 按 id 批量删除，返回实际删除的行数
    pub async fn pending_delete(&self, ids: &[i64]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut qb = delete_from!(TableName::AGENT_PENDING);
        qb.push(" WHERE id IN (");
        {
            let mut separated = qb.separated(", ");
            for id in ids {
                separated.push_bind(*id);
            }
        }
        qb.push(")");
        Ok(self.executor.execute(qb.build()).await?.rows_affected())
    }

    /// 删除实例的全部待处理记录
    pub async fn pending_delete_by_instance(&self, instance_id: i64) -> Result<()> {
        self.pending_delete_by_instances(&[instance_id]).await
    }

    /// 删除多个实例的全部待处理记录
    pub async fn pending_delete_by_instances(&self, instance_ids: &[i64]) -> Result<()> {
        if instance_ids.is_empty() {
            return Ok(());
        }
        let mut qb = delete_from!(TableName::AGENT_PENDING);
        qb.push(" WHERE instance_id IN (");
        {
            let mut separated = qb.separated(", ");
            for id in instance_ids {
                separated.push_bind(*id);
            }
        }
        qb.push(")");
        execute_allowing_zero_rows(&self.executor, qb).await
    }
}
