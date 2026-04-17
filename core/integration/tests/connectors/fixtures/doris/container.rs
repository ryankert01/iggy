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

use integration::harness::TestBinaryError;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::WaitFor::Healthcheck;
use testcontainers_modules::testcontainers::core::wait::HealthWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::sleep;

const WIREMOCK_IMAGE: &str = "wiremock/wiremock";
const WIREMOCK_TAG: &str = "3.13.2";
const WIREMOCK_PORT: u16 = 8080;
const DEFAULT_POLL_ATTEMPTS: usize = 100;
const DEFAULT_POLL_INTERVAL_MS: u64 = 100;

#[derive(Debug, Clone)]
pub struct WireMockRequest {
    pub method: String,
    pub url: String,
    pub body: String,
    pub headers: serde_json::Value,
}

impl WireMockRequest {
    pub fn body_as_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str(&self.body)
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("value"))
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
    }
}

pub struct DorisWireMockContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub base_url: String,
}

impl DorisWireMockContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let current_dir = std::env::current_dir().map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "DorisWireMockContainer".to_string(),
            message: format!("Failed to get current dir: {e}"),
        })?;

        let container = GenericImage::new(WIREMOCK_IMAGE, WIREMOCK_TAG)
            .with_exposed_port(WIREMOCK_PORT.tcp())
            .with_wait_for(Healthcheck(HealthWaitStrategy::default()))
            .with_mount(Mount::bind_mount(
                current_dir
                    .join("tests/connectors/doris/wiremock/mappings")
                    .to_string_lossy()
                    .to_string(),
                "/home/wiremock/mappings",
            ))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "DorisWireMockContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host = container
            .get_host()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "DorisWireMockContainer".to_string(),
                message: format!("Failed to get host: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(WIREMOCK_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "DorisWireMockContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        Ok(Self {
            container,
            base_url: format!("http://{host}:{host_port}"),
        })
    }

    pub async fn get_received_requests(&self) -> Result<Vec<WireMockRequest>, TestBinaryError> {
        let response = reqwest::get(format!("{}/__admin/requests", self.base_url))
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to query WireMock admin API: {e}"),
            })?;

        let body: serde_json::Value =
            response
                .json()
                .await
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to parse WireMock admin response: {e}"),
                })?;

        let empty = vec![];
        let requests = body["requests"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .map(|request| WireMockRequest {
                method: request["request"]["method"]
                    .as_str()
                    .unwrap_or("{}")
                    .to_string(),
                url: request["request"]["url"]
                    .as_str()
                    .unwrap_or("{}")
                    .to_string(),
                body: request["request"]["body"]
                    .as_str()
                    .unwrap_or("{}")
                    .to_string(),
                headers: request["request"]["headers"].clone(),
            })
            .collect();

        Ok(requests)
    }

    pub async fn wait_for_requests(
        &self,
        expected: usize,
    ) -> Result<Vec<WireMockRequest>, TestBinaryError> {
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            let requests = self.get_received_requests().await?;
            if requests.len() >= expected {
                return Ok(requests);
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected at least {expected} requests in WireMock after {} attempts",
                DEFAULT_POLL_ATTEMPTS
            ),
        })
    }

    pub async fn assert_no_requests(&self, wait_ms: u64) -> Result<(), TestBinaryError> {
        sleep(Duration::from_millis(wait_ms)).await;
        let requests = self.get_received_requests().await?;
        if requests.is_empty() {
            Ok(())
        } else {
            Err(TestBinaryError::InvalidState {
                message: format!("Expected no WireMock requests, got {}", requests.len()),
            })
        }
    }
}
