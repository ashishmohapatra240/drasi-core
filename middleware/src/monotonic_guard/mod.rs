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

mod config;
mod factory;

pub use config::MonotonicGuardConfig;
pub use factory::MonotonicGuardFactory;

use async_trait::async_trait;
use drasi_core::{
    interface::{ElementIndex, MiddlewareError, SourceMiddleware},
    models::{Element, ElementValue, SourceChange},
};

/// MonotonicGuard middleware enforces "Event-Time-Wins" semantics by preventing
/// stale events from overwriting newer state.
///
/// When an Update event arrives, this middleware:
/// 1. Looks up the current state of the element
/// 2. Compares timestamps (configured property or effective_from)
/// 3. Drops the update if the new timestamp <= old timestamp
/// 4. Passes through the update if the new timestamp > old timestamp
///
/// This prevents the "time travel glitch" where delayed events overwrite newer data.
pub struct MonotonicGuard {
    timestamp_property: String,
    fallback_to_effective_from: bool,
}

impl MonotonicGuard {
    pub fn new(config: MonotonicGuardConfig) -> Self {
        MonotonicGuard {
            timestamp_property: config.timestamp_property,
            fallback_to_effective_from: config.fallback_to_effective_from,
        }
    }

    /// Extracts a timestamp from an element, using the configured property
    /// or falling back to effective_from.
    fn extract_timestamp(&self, element: &Element) -> i64 {
        // Try to get the configured timestamp property
        if let Some(ElementValue::Integer(ts)) =
            element.get_properties().get(&self.timestamp_property)
        {
            return *ts;
        }

        // Fallback to effective_from if enabled
        if self.fallback_to_effective_from {
            return element.get_effective_from() as i64;
        }

        // If no fallback, treat as minimum timestamp (will always be older)
        i64::MIN
    }
}

#[async_trait]
impl SourceMiddleware for MonotonicGuard {
    async fn process(
        &self,
        source_change: SourceChange,
        element_index: &dyn ElementIndex,
    ) -> Result<Vec<SourceChange>, MiddlewareError> {
        // Only process Update variants - pass through everything else
        match &source_change {
            SourceChange::Update { element } => {
                let new_timestamp = self.extract_timestamp(element);
                let element_ref = element.get_reference();

                // Lookup the existing element from the index
                match element_index.get_element(element_ref).await {
                    Ok(Some(existing_element)) => {
                        // Element exists - compare timestamps
                        let old_timestamp = self.extract_timestamp(&existing_element);

                        if new_timestamp > old_timestamp {
                            // New timestamp is strictly greater - pass through
                            Ok(vec![source_change])
                        } else {
                            // New timestamp is <= old timestamp - drop (stale or duplicate)
                            Ok(vec![])
                        }
                    }
                    Ok(None) => {
                        // Element doesn't exist yet - this is effectively an insert, pass through
                        Ok(vec![source_change])
                    }
                    Err(e) => {
                        // Index error - propagate it
                        Err(MiddlewareError::IndexError(e))
                    }
                }
            }
            // Pass through Insert, Delete, and Future unchanged
            SourceChange::Insert { .. }
            | SourceChange::Delete { .. }
            | SourceChange::Future { .. } => Ok(vec![source_change]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use drasi_core::models::{ElementMetadata, ElementPropertyMap, ElementReference};
    use std::sync::Arc;

    fn create_test_element(timestamp: i64) -> Element {
        let mut props = ElementPropertyMap::default();
        props.insert("last_modified_at", ElementValue::Integer(timestamp));

        Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test_source", "test_id"),
                labels: vec![Arc::from("TestNode")].into(),
                effective_from: 0,
            },
            properties: props,
        }
    }

    #[test]
    fn test_extract_timestamp_from_property() {
        let config = MonotonicGuardConfig {
            timestamp_property: "last_modified_at".to_string(),
            fallback_to_effective_from: true,
        };
        let guard = MonotonicGuard::new(config);
        let element = create_test_element(12345);

        assert_eq!(guard.extract_timestamp(&element), 12345);
    }

    #[test]
    fn test_extract_timestamp_fallback_to_effective_from() {
        let config = MonotonicGuardConfig {
            timestamp_property: "missing_property".to_string(),
            fallback_to_effective_from: true,
        };
        let guard = MonotonicGuard::new(config);

        let element = Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test_source", "test_id"),
                labels: vec![Arc::from("TestNode")].into(),
                effective_from: 99999,
            },
            properties: ElementPropertyMap::default(),
        };

        assert_eq!(guard.extract_timestamp(&element), 99999);
    }

    #[test]
    fn test_extract_timestamp_no_fallback() {
        let config = MonotonicGuardConfig {
            timestamp_property: "missing_property".to_string(),
            fallback_to_effective_from: false,
        };
        let guard = MonotonicGuard::new(config);

        let element = Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test_source", "test_id"),
                labels: vec![Arc::from("TestNode")].into(),
                effective_from: 99999,
            },
            properties: ElementPropertyMap::default(),
        };

        assert_eq!(guard.extract_timestamp(&element), i64::MIN);
    }
}
