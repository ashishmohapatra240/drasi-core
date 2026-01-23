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

use drasi_core::{
    interface::{MiddlewareSetupError, SourceMiddleware, SourceMiddlewareFactory},
    models::SourceMiddlewareConfig,
};

use super::{MonotonicGuard, MonotonicGuardConfig};

/// Factory for creating MonotonicGuard middleware instances
pub struct MonotonicGuardFactory;

impl MonotonicGuardFactory {
    pub fn new() -> Self {
        MonotonicGuardFactory
    }
}

impl Default for MonotonicGuardFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceMiddlewareFactory for MonotonicGuardFactory {
    fn name(&self) -> String {
        "monotonic-guard".to_string()
    }

    fn create(
        &self,
        config: &SourceMiddlewareConfig,
    ) -> Result<Arc<dyn SourceMiddleware>, MiddlewareSetupError> {
        // Parse the configuration from the JSON config object
        let guard_config: MonotonicGuardConfig = serde_json::from_value(serde_json::Value::Object(
            config.config.clone(),
        ))
        .map_err(|e| {
            MiddlewareSetupError::InvalidConfiguration(format!(
                "Failed to parse MonotonicGuard configuration: {}",
                e
            ))
        })?;

        // Validate that timestamp_property is not empty
        if guard_config.timestamp_property.is_empty() {
            return Err(MiddlewareSetupError::InvalidConfiguration(
                "timestamp_property cannot be empty".to_string(),
            ));
        }

        // Create and return the middleware
        Ok(Arc::new(MonotonicGuard::new(guard_config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_factory_create_valid_config() {
        let factory = MonotonicGuardFactory::new();
        assert_eq!(factory.name(), "monotonic-guard");

        let config = SourceMiddlewareConfig {
            kind: Arc::from("monotonic-guard"),
            name: Arc::from("test_guard"),
            config: json!({
                "timestamp_property": "last_modified_at",
                "fallback_to_effective_from": true
            })
            .as_object()
            .unwrap()
            .clone(),
        };

        let result = factory.create(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_create_with_defaults() {
        let factory = MonotonicGuardFactory::new();

        let config = SourceMiddlewareConfig {
            kind: Arc::from("monotonic-guard"),
            name: Arc::from("test_guard"),
            config: json!({
                "timestamp_property": "created_at"
            })
            .as_object()
            .unwrap()
            .clone(),
        };

        let result = factory.create(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_create_empty_timestamp_property() {
        let factory = MonotonicGuardFactory::new();

        let config = SourceMiddlewareConfig {
            kind: Arc::from("monotonic-guard"),
            name: Arc::from("test_guard"),
            config: json!({
                "timestamp_property": ""
            })
            .as_object()
            .unwrap()
            .clone(),
        };

        let result = factory.create(&config);
        assert!(result.is_err());
        match result {
            Err(MiddlewareSetupError::InvalidConfiguration(msg)) => {
                assert!(msg.contains("cannot be empty"));
            }
            _ => panic!("Expected InvalidConfiguration error"),
        }
    }

    #[test]
    fn test_factory_create_invalid_config() {
        let factory = MonotonicGuardFactory::new();

        let config = SourceMiddlewareConfig {
            kind: Arc::from("monotonic-guard"),
            name: Arc::from("test_guard"),
            config: json!({
                "invalid_field": "value"
            })
            .as_object()
            .unwrap()
            .clone(),
        };

        let result = factory.create(&config);
        assert!(result.is_err());
    }
}
