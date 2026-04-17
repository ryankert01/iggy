/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

sink_connector!(DorisSink);

const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "30s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_MAX_CONNECTIONS: usize = 10;

#[derive(Debug)]
pub struct DorisSink {
    id: u32,
    config: DorisSinkConfig,
    stream_load_url: Option<String>,
    client: Option<reqwest::Client>,
    timeout: Duration,
    retry_delay: Duration,
    max_retry_delay: Duration,
    retry_backoff_multiplier: u32,
    max_retries: u32,
    batch_size: usize,
    include_metadata: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
    success_status_codes: HashSet<u16>,
    verbose: bool,
    messages_processed: AtomicU64,
    requests_sent: AtomicU64,
    errors_count: AtomicU64,
    label_counter: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DorisSinkConfig {
    pub url: String,
    pub database: String,
    pub table: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub timeout: Option<String>,
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub retry_backoff_multiplier: Option<u32>,
    pub max_retry_delay: Option<String>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub label_prefix: Option<String>,
    pub columns: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub success_status_codes: Option<Vec<u16>>,
    pub max_connections: Option<usize>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DorisStreamLoadResponse {
    #[serde(rename = "Status")]
    status: Option<String>,
    #[serde(rename = "Message")]
    message: Option<String>,
}

impl DorisStreamLoadResponse {
    fn is_success(&self) -> bool {
        let Some(status) = &self.status else {
            return false;
        };
        matches!(
            status.to_ascii_lowercase().as_str(),
            "success" | "publish timeout" | "label already exists"
        )
    }
}

impl DorisSink {
    pub fn new(id: u32, config: DorisSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let timeout = parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        let retry_backoff_multiplier = config.retry_backoff_multiplier.unwrap_or(2).max(1);
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize;
        let include_metadata = config.include_metadata.unwrap_or(true);
        let include_checksum = config.include_checksum.unwrap_or(false);
        let include_origin_timestamp = config.include_origin_timestamp.unwrap_or(false);
        let success_status_codes = config
            .success_status_codes
            .clone()
            .unwrap_or_else(|| vec![200])
            .into_iter()
            .collect();

        DorisSink {
            id,
            config,
            stream_load_url: None,
            client: None,
            timeout,
            retry_delay,
            max_retry_delay,
            retry_backoff_multiplier,
            max_retries,
            batch_size,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            success_status_codes,
            verbose,
            messages_processed: AtomicU64::new(0),
            requests_sent: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
            label_counter: AtomicU64::new(0),
        }
    }

    fn stream_load_url(base_url: &str, database: &str, table: &str) -> Result<String, Error> {
        if base_url.trim().is_empty() {
            return Err(Error::InitError(
                "Doris sink URL is empty — 'url' is required".to_string(),
            ));
        }
        if database.trim().is_empty() {
            return Err(Error::InitError(
                "Doris sink database is empty — 'database' is required".to_string(),
            ));
        }
        if table.trim().is_empty() {
            return Err(Error::InitError(
                "Doris sink table is empty — 'table' is required".to_string(),
            ));
        }

        let parsed = reqwest::Url::parse(base_url).map_err(|e| {
            Error::InitError(format!("Doris sink URL '{}' is not valid: {}", base_url, e))
        })?;

        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(Error::InitError(format!(
                "Doris sink URL scheme '{}' is not allowed — only 'http' and 'https' are supported",
                scheme
            )));
        }

        let trimmed = base_url.trim_end_matches('/');
        Ok(format!("{trimmed}/api/{database}/{table}/_stream_load"))
    }

    fn build_client(&self) -> Result<reqwest::Client, Error> {
        let max_connections = self
            .config
            .max_connections
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);
        reqwest::Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(max_connections)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))
    }

    fn validate_config(&self) -> Result<(), Error> {
        if self.success_status_codes.is_empty() {
            return Err(Error::InitError(
                "success_status_codes must not be empty".to_string(),
            ));
        }

        for &code in &self.success_status_codes {
            if !(200..=599).contains(&code) {
                return Err(Error::InitError(format!(
                    "Invalid status code {} in success_status_codes — must be 200-599",
                    code
                )));
            }
        }

        if self.retry_delay > self.max_retry_delay {
            return Err(Error::InitError(format!(
                "retry_delay ({:?}) cannot exceed max_retry_delay ({:?})",
                self.retry_delay, self.max_retry_delay
            )));
        }

        if let Some(headers) = &self.config.headers {
            for (key, value) in headers {
                reqwest::header::HeaderName::from_bytes(key.as_bytes()).map_err(|e| {
                    Error::InitError(format!("Invalid header name '{}': {}", key, e))
                })?;
                reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                    Error::InitError(format!("Invalid header value for '{}': {}", key, e))
                })?;
            }
        }

        Ok(())
    }

    fn label_prefix(&self) -> String {
        self.config
            .label_prefix
            .clone()
            .filter(|prefix| !prefix.trim().is_empty())
            .unwrap_or_else(|| format!("iggy-doris-{}", self.id))
    }

    fn build_batch_payload(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Vec<Value> {
        let mut records = Vec::with_capacity(messages.len());

        for message in messages {
            let Payload::Json(json_value) = &message.payload else {
                if self.verbose {
                    warn!(
                        "Doris sink ID: {} skipped non-JSON payload at offset {}",
                        self.id, message.offset
                    );
                }
                continue;
            };

            let payload = serde_json::to_value(json_value).unwrap_or(Value::Null);
            if self.include_metadata {
                let mut metadata = Map::new();
                metadata.insert(
                    "iggy_stream".to_string(),
                    Value::String(topic_metadata.stream.clone()),
                );
                metadata.insert(
                    "iggy_topic".to_string(),
                    Value::String(topic_metadata.topic.clone()),
                );
                metadata.insert(
                    "iggy_partition_id".to_string(),
                    Value::from(messages_metadata.partition_id),
                );
                metadata.insert("iggy_offset".to_string(), Value::from(message.offset));
                metadata.insert("iggy_id".to_string(), Value::String(message.id.to_string()));
                metadata.insert("iggy_timestamp".to_string(), Value::from(message.timestamp));

                if self.include_checksum {
                    metadata.insert("iggy_checksum".to_string(), Value::from(message.checksum));
                }

                if self.include_origin_timestamp {
                    metadata.insert(
                        "iggy_origin_timestamp".to_string(),
                        Value::from(message.origin_timestamp),
                    );
                }

                let mut envelope = Map::new();
                envelope.insert("metadata".to_string(), Value::Object(metadata));
                envelope.insert("payload".to_string(), payload);
                records.push(Value::Object(envelope));
            } else {
                records.push(payload);
            }
        }

        records
    }

    fn get_client(&self) -> Result<&reqwest::Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Connection("Doris HTTP client is not initialized".to_string()))
    }

    fn get_stream_load_url(&self) -> Result<&str, Error> {
        self.stream_load_url.as_deref().ok_or_else(|| {
            Error::Connection("Doris stream load URL is not initialized".to_string())
        })
    }

    fn is_transient_status(status: StatusCode) -> bool {
        matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
    }

    fn auth_header_value(&self) -> Option<String> {
        let Some(username) = self.config.username.as_deref() else {
            return None;
        };
        let password = self.config.password.as_deref().unwrap_or_default();
        let token = general_purpose::STANDARD.encode(format!("{username}:{password}"));
        Some(format!("Basic {token}"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_batch(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        batch: &[ConsumedMessage],
        batch_index: usize,
    ) -> Result<(), Error> {
        let records = self.build_batch_payload(topic_metadata, messages_metadata, batch);
        if records.is_empty() {
            return Ok(());
        }

        let body = serde_json::to_vec(&records)
            .map_err(|e| Error::Serialization(format!("Failed to serialize batch payload: {e}")))?;

        let label = format!(
            "{}-{}-{}-{}",
            self.label_prefix(),
            self.label_counter.fetch_add(1, Ordering::Relaxed),
            batch_index,
            Uuid::now_v7()
        );

        let client = self.get_client()?;
        let url = self.get_stream_load_url()?;

        let mut attempt = 0u32;
        let mut current_delay = self.retry_delay;
        loop {
            attempt += 1;
            self.requests_sent.fetch_add(1, Ordering::Relaxed);

            let mut request = client
                .put(url)
                .header("format", "json")
                .header("strip_outer_array", "true")
                .header("label", label.clone())
                .header("Content-Type", "application/json")
                .header("Expect", "100-continue")
                .body(body.clone());

            if let Some(columns) = &self.config.columns {
                request = request.header("columns", columns);
            }

            if let Some(auth_value) = self.auth_header_value() {
                request = request.header("Authorization", auth_value);
            }

            if let Some(headers) = &self.config.headers {
                for (key, value) in headers {
                    if key.eq_ignore_ascii_case("content-type") {
                        continue;
                    }
                    request = request.header(key, value);
                }
            }

            let response = request.send().await;
            match response {
                Ok(response) => {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();

                    if !self.success_status_codes.contains(&status.as_u16()) {
                        if Self::is_transient_status(status) && attempt <= self.max_retries {
                            warn!(
                                "Doris sink ID: {} got transient HTTP status {} (attempt {}/{}), retrying...",
                                self.id,
                                status,
                                attempt,
                                self.max_retries + 1
                            );
                            tokio::time::sleep(current_delay).await;
                            current_delay = (current_delay * self.retry_backoff_multiplier)
                                .min(self.max_retry_delay);
                            continue;
                        }

                        self.errors_count.fetch_add(1, Ordering::Relaxed);
                        return Err(Error::PermanentHttpError(format!(
                            "Doris stream load failed with status {}: {}",
                            status.as_u16(),
                            text
                        )));
                    }

                    if !text.trim().is_empty() {
                        match serde_json::from_str::<DorisStreamLoadResponse>(&text) {
                            Ok(parsed) => {
                                if !parsed.is_success() {
                                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                                    let message = parsed.message.unwrap_or_else(|| text.clone());
                                    return Err(Error::PermanentHttpError(format!(
                                        "Doris stream load reported failure: {}",
                                        message
                                    )));
                                }
                            }
                            Err(_) => {
                                if self.verbose {
                                    debug!(
                                        "Doris sink ID: {} received non-JSON stream load response: {}",
                                        self.id, text
                                    );
                                }
                            }
                        }
                    }

                    self.messages_processed
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    return Ok(());
                }
                Err(error) => {
                    if (error.is_timeout() || error.is_connect()) && attempt <= self.max_retries {
                        warn!(
                            "Doris sink ID: {} transient request error (attempt {}/{}): {}",
                            self.id,
                            attempt,
                            self.max_retries + 1,
                            error
                        );
                        tokio::time::sleep(current_delay).await;
                        current_delay = (current_delay * self.retry_backoff_multiplier)
                            .min(self.max_retry_delay);
                        continue;
                    }

                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::HttpRequestFailed(format!(
                        "Doris stream load request failed: {}",
                        error
                    )));
                }
            }
        }
    }
}

#[async_trait]
impl Sink for DorisSink {
    async fn open(&mut self) -> Result<(), Error> {
        self.validate_config()?;

        let stream_load_url =
            Self::stream_load_url(&self.config.url, &self.config.database, &self.config.table)?;

        self.stream_load_url = Some(stream_load_url);
        self.client = Some(self.build_client()?);

        info!(
            "Opened Doris sink connector with ID: {} (database: {}, table: {}, batch_size: {})",
            self.id, self.config.database, self.config.table, self.batch_size
        );

        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        if self.verbose {
            debug!(
                "Doris sink ID: {} received {} messages (schema: {}, stream: {}, topic: {})",
                self.id,
                messages.len(),
                messages_metadata.schema,
                topic_metadata.stream,
                topic_metadata.topic,
            );
        }

        for (index, batch) in messages.chunks(self.batch_size).enumerate() {
            self.send_batch(topic_metadata, &messages_metadata, batch, index)
                .await?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!(
            "Doris sink connector ID: {} closed. Stats: {} messages processed, {} requests sent, {} errors.",
            self.id,
            self.messages_processed.load(Ordering::Relaxed),
            self.requests_sent.load(Ordering::Relaxed),
            self.errors_count.load(Ordering::Relaxed),
        );

        self.client = None;
        self.stream_load_url = None;
        Ok(())
    }
}

fn parse_duration(value: Option<&str>, default_value: &str) -> Duration {
    let raw = value.unwrap_or(default_value);
    HumanDuration::from_str(raw)
        .map(Duration::from)
        .unwrap_or_else(|_| Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DorisSinkConfig {
        DorisSinkConfig {
            url: "http://localhost:8030".to_string(),
            database: "events".to_string(),
            table: "messages".to_string(),
            username: Some("root".to_string()),
            password: Some("pwd".to_string()),
            timeout: None,
            batch_size: None,
            max_retries: None,
            retry_delay: None,
            retry_backoff_multiplier: None,
            max_retry_delay: None,
            include_metadata: None,
            include_checksum: None,
            include_origin_timestamp: None,
            label_prefix: None,
            columns: None,
            headers: None,
            success_status_codes: None,
            max_connections: None,
            verbose_logging: None,
        }
    }

    #[test]
    fn given_valid_url_database_and_table_should_build_stream_load_url() {
        let url = DorisSink::stream_load_url("http://localhost:8030/", "db", "tbl").unwrap();
        assert_eq!(url, "http://localhost:8030/api/db/tbl/_stream_load");
    }

    #[test]
    fn given_invalid_scheme_should_fail() {
        let error = DorisSink::stream_load_url("ftp://localhost", "db", "tbl").unwrap_err();
        assert!(matches!(error, Error::InitError(_)));
    }

    #[test]
    fn given_success_response_status_should_be_success() {
        let response = DorisStreamLoadResponse {
            status: Some("Success".to_string()),
            message: None,
        };
        assert!(response.is_success());
    }

    #[test]
    fn given_publish_timeout_response_status_should_be_success() {
        let response = DorisStreamLoadResponse {
            status: Some("Publish Timeout".to_string()),
            message: None,
        };
        assert!(response.is_success());
    }

    #[test]
    fn given_label_already_exists_response_status_should_be_success() {
        let response = DorisStreamLoadResponse {
            status: Some("Label Already Exists".to_string()),
            message: None,
        };
        assert!(response.is_success());
    }

    #[test]
    fn given_failed_response_status_should_not_be_success() {
        let response = DorisStreamLoadResponse {
            status: Some("Fail".to_string()),
            message: Some("invalid data".to_string()),
        };
        assert!(!response.is_success());
    }

    #[test]
    fn given_retryable_status_codes_should_be_transient() {
        assert!(DorisSink::is_transient_status(
            StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(DorisSink::is_transient_status(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(DorisSink::is_transient_status(StatusCode::BAD_GATEWAY));
        assert!(DorisSink::is_transient_status(
            StatusCode::SERVICE_UNAVAILABLE
        ));
    }

    #[test]
    fn given_non_retryable_status_codes_should_not_be_transient() {
        assert!(!DorisSink::is_transient_status(StatusCode::BAD_REQUEST));
        assert!(!DorisSink::is_transient_status(StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn given_username_password_should_build_basic_auth_header() {
        let sink = DorisSink::new(1, config());
        let auth = sink.auth_header_value().unwrap();
        assert!(auth.starts_with("Basic "));
    }

    #[tokio::test]
    async fn given_invalid_config_should_fail_open() {
        let mut cfg = config();
        cfg.table = "".to_string();
        let mut sink = DorisSink::new(1, cfg);
        let result = sink.open().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_valid_config_should_open() {
        let mut sink = DorisSink::new(1, config());
        sink.open().await.unwrap();
        assert!(sink.client.is_some());
        assert!(sink.stream_load_url.is_some());
    }
}
