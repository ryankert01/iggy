/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use super::container::DorisWireMockContainer;
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;

const DEFAULT_TEST_STREAM: &str = "test_stream";
const DEFAULT_TEST_TOPIC: &str = "test_topic";
const DEFAULT_TEST_TOPIC_2: &str = "test_topic_2";

const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_DORIS_PATH";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_CONSUMER_GROUP";

const ENV_SINK_URL: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_URL";
const ENV_SINK_DATABASE: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_DATABASE";
const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_TABLE";
const ENV_SINK_USERNAME: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_USERNAME";
const ENV_SINK_PASSWORD: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_PASSWORD";
const ENV_SINK_BATCH_SIZE: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_BATCH_SIZE";
const ENV_SINK_MAX_RETRIES: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_MAX_RETRIES";
const ENV_SINK_RETRY_DELAY: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_RETRY_DELAY";
const ENV_SINK_INCLUDE_METADATA: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_INCLUDE_METADATA";
const ENV_SINK_VERBOSE_LOGGING: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_VERBOSE_LOGGING";

pub struct DorisSinkFixture {
    container: DorisWireMockContainer,
}

impl DorisSinkFixture {
    pub fn container(&self) -> &DorisWireMockContainer {
        &self.container
    }

    fn base_envs(container: &DorisWireMockContainer) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_doris_sink".to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{}]", DEFAULT_TEST_TOPIC),
        );
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string());
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "doris_sink_cg".to_string(),
        );
        envs.insert(ENV_SINK_URL.to_string(), container.base_url.clone());
        envs.insert(ENV_SINK_DATABASE.to_string(), "events".to_string());
        envs.insert(ENV_SINK_TABLE.to_string(), "iggy_messages".to_string());
        envs.insert(ENV_SINK_USERNAME.to_string(), "root".to_string());
        envs.insert(ENV_SINK_PASSWORD.to_string(), "password".to_string());
        envs.insert(ENV_SINK_BATCH_SIZE.to_string(), "100".to_string());
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "1".to_string());
        envs.insert(ENV_SINK_RETRY_DELAY.to_string(), "100ms".to_string());
        envs.insert(ENV_SINK_INCLUDE_METADATA.to_string(), "true".to_string());
        envs.insert(ENV_SINK_VERBOSE_LOGGING.to_string(), "true".to_string());
        envs
    }
}

#[async_trait]
impl TestFixture for DorisSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            container: DorisWireMockContainer::start().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        Self::base_envs(&self.container)
    }
}

pub struct DorisSinkRetryFixture {
    inner: DorisSinkFixture,
}

impl DorisSinkRetryFixture {
    pub fn container(&self) -> &DorisWireMockContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkRetryFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: DorisSinkFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = DorisSinkFixture::base_envs(self.inner.container());
        envs.insert(ENV_SINK_DATABASE.to_string(), "events_retry".to_string());
        envs.insert(ENV_SINK_MAX_RETRIES.to_string(), "2".to_string());
        envs
    }
}

pub struct DorisSinkNoMetadataFixture {
    inner: DorisSinkFixture,
}

impl DorisSinkNoMetadataFixture {
    pub fn container(&self) -> &DorisWireMockContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkNoMetadataFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: DorisSinkFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = DorisSinkFixture::base_envs(self.inner.container());
        envs.insert(ENV_SINK_INCLUDE_METADATA.to_string(), "false".to_string());
        envs
    }
}

pub struct DorisSinkRawPayloadFixture {
    inner: DorisSinkFixture,
}

impl DorisSinkRawPayloadFixture {
    pub fn container(&self) -> &DorisWireMockContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkRawPayloadFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: DorisSinkFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = DorisSinkFixture::base_envs(self.inner.container());
        envs.insert(ENV_SINK_STREAMS_0_SCHEMA.to_string(), "raw".to_string());
        envs
    }
}

pub struct DorisSinkMultiTopicFixture {
    inner: DorisSinkFixture,
}

impl DorisSinkMultiTopicFixture {
    pub fn container(&self) -> &DorisWireMockContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkMultiTopicFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: DorisSinkFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = DorisSinkFixture::base_envs(self.inner.container());
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{},{}]", DEFAULT_TEST_TOPIC, DEFAULT_TEST_TOPIC_2),
        );
        envs
    }
}
