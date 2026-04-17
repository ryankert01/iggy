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

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::{
    DorisSinkFixture, DorisSinkMultiTopicFixture, DorisSinkNoMetadataFixture,
    DorisSinkRawPayloadFixture, DorisSinkRetryFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_are_stream_loaded(harness: &TestHarness, fixture: DorisSinkFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payloads = vec![
        serde_json::json!({"id": 1, "name": "Alice"}),
        serde_json::json!({"id": 2, "name": "Bob"}),
        serde_json::json!({"id": 3, "name": "Carol"}),
    ];

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(serde_json::to_vec(payload).unwrap()))
                .build()
                .unwrap()
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let requests = fixture.container().wait_for_requests(1).await.unwrap();
    let first = requests.first().unwrap();
    assert_eq!(first.method, "PUT");
    assert_eq!(first.url, "/api/events/iggy_messages/_stream_load");

    let body = first.body_as_json().unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), TEST_MESSAGE_COUNT);
    assert!(arr[0].get("metadata").is_some());
    assert!(arr[0].get("payload").is_some());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn transient_error_is_retried(harness: &TestHarness, fixture: DorisSinkRetryFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({"retry": true})).unwrap(),
            ))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let requests = fixture.container().wait_for_requests(2).await.unwrap();
    assert!(
        requests
            .iter()
            .all(|request| request.url == "/api/events_retry/iggy_messages/_stream_load")
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn raw_payloads_are_ignored(harness: &TestHarness, fixture: DorisSinkRawPayloadFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from_static(b"hello"))
            .build()
            .unwrap(),
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from_static(b"world"))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    fixture.container().assert_no_requests(2000).await.unwrap();
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn metadata_can_be_disabled(harness: &TestHarness, fixture: DorisSinkNoMetadataFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payload = serde_json::json!({"simple": true, "value": 10});
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(serde_json::to_vec(&payload).unwrap()))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    let requests = fixture.container().wait_for_requests(1).await.unwrap();
    let body = requests[0].body_as_json().unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0], payload);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn messages_from_multiple_topics_are_delivered(
    harness: &TestHarness,
    fixture: DorisSinkMultiTopicFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_1: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let topic_2: Identifier = seeds::names::TOPIC_2.try_into().unwrap();

    let mut topic_1_messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({"topic": "one"})).unwrap(),
            ))
            .build()
            .unwrap(),
    ];

    let mut topic_2_messages = vec![
        IggyMessage::builder()
            .id(2)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({"topic": "two"})).unwrap(),
            ))
            .build()
            .unwrap(),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_1,
            &Partitioning::partition_id(0),
            &mut topic_1_messages,
        )
        .await
        .unwrap();

    client
        .send_messages(
            &stream_id,
            &topic_2,
            &Partitioning::partition_id(0),
            &mut topic_2_messages,
        )
        .await
        .unwrap();

    let requests = fixture.container().wait_for_requests(2).await.unwrap();
    let mut seen_topics = std::collections::HashSet::new();

    for request in requests {
        let body = request.body_as_json().unwrap();
        let first = body.as_array().unwrap().first().unwrap();
        let topic = first["metadata"]["iggy_topic"].as_str().unwrap();
        seen_topics.insert(topic.to_string());
    }

    assert!(seen_topics.contains(seeds::names::TOPIC));
    assert!(seen_topics.contains(seeds::names::TOPIC_2));
}
