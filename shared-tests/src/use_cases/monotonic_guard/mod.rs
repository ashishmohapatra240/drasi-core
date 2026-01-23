// Copyright 2024 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use drasi_middleware::monotonic_guard::MonotonicGuardFactory;
use serde_json::json;

use drasi_core::{
    evaluation::functions::FunctionRegistry,
    middleware::MiddlewareTypeRegistry,
    models::{
        Element, ElementMetadata, ElementPropertyMap, ElementReference, ElementValue, SourceChange,
    },
    query::{ContinuousQuery, QueryBuilder},
};
use drasi_functions_cypher::CypherFunctionSet;
use drasi_query_cypher::CypherParser;

use crate::QueryTestConfig;

// Simple observer query that returns all nodes
const OBSERVER_QUERY: &str = r#"
    MATCH (n:Sensor)
    RETURN n.id as id, n.temperature as temperature, n.last_modified_at as timestamp
"#;

fn create_middleware_registry() -> Arc<MiddlewareTypeRegistry> {
    let mut registry = MiddlewareTypeRegistry::new();
    registry.register(Arc::new(MonotonicGuardFactory::new()));
    Arc::new(registry)
}

fn create_monotonic_guard_config(
    name: &str,
    timestamp_property: &str,
    fallback_to_effective_from: bool,
) -> Arc<drasi_core::models::SourceMiddlewareConfig> {
    Arc::new(drasi_core::models::SourceMiddlewareConfig {
        kind: Arc::from("monotonic-guard"),
        name: Arc::from(name),
        config: json!({
            "timestamp_property": timestamp_property,
            "fallback_to_effective_from": fallback_to_effective_from
        })
        .as_object()
        .unwrap()
        .clone(),
    })
}

async fn setup_query(
    config: &(impl QueryTestConfig + Send),
    middleware_name: &str,
    timestamp_property: &str,
    fallback_to_effective_from: bool,
) -> ContinuousQuery {
    let registry = create_middleware_registry();
    let function_registry = Arc::new(FunctionRegistry::new()).with_cypher_function_set();
    let parser = Arc::new(CypherParser::new(function_registry.clone()));
    let mut builder =
        QueryBuilder::new(OBSERVER_QUERY, parser).with_function_registry(function_registry);

    builder = config.config_query(builder).await;
    builder = builder.with_middleware_registry(registry);

    let mw_config = create_monotonic_guard_config(
        middleware_name,
        timestamp_property,
        fallback_to_effective_from,
    );

    builder = builder.with_source_middleware(mw_config);
    builder = builder.with_source_pipeline("test_source", &[middleware_name.to_string()]);

    builder.build().await
}

fn create_sensor_insert(id: &str, temperature: i64, timestamp: i64) -> SourceChange {
    let mut props = ElementPropertyMap::default();
    props.insert("id", ElementValue::String(Arc::from(id)));
    props.insert("temperature", ElementValue::Integer(temperature));
    props.insert("last_modified_at", ElementValue::Integer(timestamp));

    SourceChange::Insert {
        element: Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test_source", id),
                labels: vec![Arc::from("Sensor")].into(),
                effective_from: timestamp as u64,
            },
            properties: props,
        },
    }
}

fn create_sensor_update(id: &str, temperature: i64, timestamp: i64) -> SourceChange {
    let mut props = ElementPropertyMap::default();
    props.insert("id", ElementValue::String(Arc::from(id)));
    props.insert("temperature", ElementValue::Integer(temperature));
    props.insert("last_modified_at", ElementValue::Integer(timestamp));

    SourceChange::Update {
        element: Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test_source", id),
                labels: vec![Arc::from("Sensor")].into(),
                effective_from: timestamp as u64,
            },
            properties: props,
        },
    }
}

fn create_sensor_update_no_timestamp(
    id: &str,
    temperature: i64,
    effective_from: u64,
) -> SourceChange {
    let mut props = ElementPropertyMap::default();
    props.insert("id", ElementValue::String(Arc::from(id)));
    props.insert("temperature", ElementValue::Integer(temperature));

    SourceChange::Update {
        element: Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test_source", id),
                labels: vec![Arc::from("Sensor")].into(),
                effective_from,
            },
            properties: props,
        },
    }
}

fn create_sensor_delete(id: &str, timestamp: u64) -> SourceChange {
    SourceChange::Delete {
        metadata: ElementMetadata {
            reference: ElementReference::new("test_source", id),
            labels: vec![Arc::from("Sensor")].into(),
            effective_from: timestamp,
        },
    }
}

/// Test 1: New entity (no prior state) should pass through
#[allow(clippy::unwrap_used)]
async fn test_new_entity(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert a new sensor
    let insert = create_sensor_insert("sensor1", 25, 100);
    let result = query.process_source_change(insert).await.unwrap();

    // Should have 1 insert result
    assert_eq!(result.len(), 1);
}

/// Test 2: Valid update (newer timestamp) should pass through
#[allow(clippy::unwrap_used)]
async fn test_valid_update_newer_timestamp(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert initial sensor
    let insert = create_sensor_insert("sensor1", 25, 100);
    query.process_source_change(insert).await.unwrap();

    // Update with newer timestamp
    let update = create_sensor_update("sensor1", 30, 200);
    let result = query.process_source_change(update).await.unwrap();

    // Should have 1 update result (change detected)
    assert_eq!(result.len(), 1);
}

/// Test 3: Stale update (older timestamp) should be dropped
#[allow(clippy::unwrap_used)]
async fn test_stale_update_older_timestamp(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert initial sensor
    let insert = create_sensor_insert("sensor1", 25, 200);
    query.process_source_change(insert).await.unwrap();

    // Try to update with older timestamp (stale event)
    let update = create_sensor_update("sensor1", 30, 100);
    let result = query.process_source_change(update).await.unwrap();

    // Should have 0 results (update was dropped by middleware)
    assert_eq!(result.len(), 0);
}

/// Test 4: Equal timestamp should be dropped (idempotency)
#[allow(clippy::unwrap_used)]
async fn test_equal_timestamp_dropped(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert initial sensor
    let insert = create_sensor_insert("sensor1", 25, 150);
    query.process_source_change(insert).await.unwrap();

    // Try to update with same timestamp
    let update = create_sensor_update("sensor1", 30, 150);
    let result = query.process_source_change(update).await.unwrap();

    // Should have 0 results (duplicate timestamp dropped)
    assert_eq!(result.len(), 0);
}

/// Test 5: Missing timestamp property should fallback to effective_from
#[allow(clippy::unwrap_used)]
async fn test_missing_timestamp_property_fallback(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert sensor with timestamp
    let insert = create_sensor_insert("sensor1", 25, 100);
    query.process_source_change(insert).await.unwrap();

    // Update without timestamp property, but with newer effective_from
    let update = create_sensor_update_no_timestamp("sensor1", 30, 200);
    let result = query.process_source_change(update).await.unwrap();

    // Should pass through (fallback to effective_from worked)
    assert_eq!(result.len(), 1);

    // Try another update with older effective_from
    let stale_update = create_sensor_update_no_timestamp("sensor1", 35, 150);
    let result = query.process_source_change(stale_update).await.unwrap();

    // Should be dropped (stale based on effective_from)
    assert_eq!(result.len(), 0);
}

/// Test 6: Multiple updates in sequence should maintain monotonicity
#[allow(clippy::unwrap_used)]
async fn test_multiple_updates_sequence(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert initial sensor
    let insert = create_sensor_insert("sensor1", 20, 100);
    query.process_source_change(insert).await.unwrap();

    // Valid update 1: 100 -> 150
    let update1 = create_sensor_update("sensor1", 25, 150);
    let result = query.process_source_change(update1).await.unwrap();
    assert_eq!(result.len(), 1); // Should pass

    // Valid update 2: 150 -> 200
    let update2 = create_sensor_update("sensor1", 30, 200);
    let result = query.process_source_change(update2).await.unwrap();
    assert_eq!(result.len(), 1); // Should pass

    // Stale update: 200 -> 120 (out of order)
    let update3 = create_sensor_update("sensor1", 22, 120);
    let result = query.process_source_change(update3).await.unwrap();
    assert_eq!(result.len(), 0); // Should be dropped

    // Valid update 3: 200 -> 250
    let update4 = create_sensor_update("sensor1", 35, 250);
    let result = query.process_source_change(update4).await.unwrap();
    assert_eq!(result.len(), 1); // Should pass
}

/// Test 7: Insert operations should pass through unchanged
#[allow(clippy::unwrap_used)]
async fn test_insert_operations_pass_through(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Multiple inserts should all pass through
    let insert1 = create_sensor_insert("sensor1", 20, 100);
    let result = query.process_source_change(insert1).await.unwrap();
    assert_eq!(result.len(), 1);

    let insert2 = create_sensor_insert("sensor2", 25, 200);
    let result = query.process_source_change(insert2).await.unwrap();
    assert_eq!(result.len(), 1);
}

/// Test 8: Delete operations should pass through unchanged
#[allow(clippy::unwrap_used)]
async fn test_delete_operations_pass_through(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert sensor first
    let insert = create_sensor_insert("sensor1", 25, 100);
    query.process_source_change(insert).await.unwrap();

    // Delete should pass through
    let delete = create_sensor_delete("sensor1", 200);
    let result = query.process_source_change(delete).await.unwrap();
    assert_eq!(result.len(), 1);
}

/// Test 9: Different sources should have independent monotonicity tracking
#[allow(clippy::unwrap_used)]
async fn test_different_sources_independent(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", true).await;

    // Insert sensor1
    let insert1 = create_sensor_insert("sensor1", 20, 100);
    query.process_source_change(insert1).await.unwrap();

    // Insert sensor2 (different entity)
    let insert2 = create_sensor_insert("sensor2", 30, 50);
    query.process_source_change(insert2).await.unwrap();

    // Update sensor1 with newer timestamp
    let update1 = create_sensor_update("sensor1", 25, 200);
    let result = query.process_source_change(update1).await.unwrap();
    assert_eq!(result.len(), 1); // Should pass

    // Update sensor2 with older timestamp (but it should still work because it's independent)
    let update2 = create_sensor_update("sensor2", 35, 100);
    let result = query.process_source_change(update2).await.unwrap();
    assert_eq!(result.len(), 1); // Should pass (100 > 50)
}

/// Test 10: No fallback - missing timestamp property should result in dropped updates
#[allow(clippy::unwrap_used)]
async fn test_no_fallback_missing_timestamp(config: &(impl QueryTestConfig + Send)) {
    let query = setup_query(config, "monotonic_guard", "last_modified_at", false).await;

    // Insert sensor with timestamp
    let insert = create_sensor_insert("sensor1", 25, 100);
    query.process_source_change(insert).await.unwrap();

    // Update without timestamp property and no fallback
    let update = create_sensor_update_no_timestamp("sensor1", 30, 200);
    let result = query.process_source_change(update).await.unwrap();

    // Should be dropped because timestamp property is missing and fallback is disabled
    assert_eq!(result.len(), 0);
}

// Export test functions for different configurations
pub async fn monotonic_guard_tests(config: &(impl QueryTestConfig + Send)) {
    test_new_entity(config).await;
    test_valid_update_newer_timestamp(config).await;
    test_stale_update_older_timestamp(config).await;
    test_equal_timestamp_dropped(config).await;
    test_missing_timestamp_property_fallback(config).await;
    test_multiple_updates_sequence(config).await;
    test_insert_operations_pass_through(config).await;
    test_delete_operations_pass_through(config).await;
    test_different_sources_independent(config).await;
    test_no_fallback_missing_timestamp(config).await;
}
