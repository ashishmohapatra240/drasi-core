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

use serde::Deserialize;

/// Configuration for the MonotonicGuard middleware
#[derive(Debug, Clone, Deserialize)]
pub struct MonotonicGuardConfig {
    /// The property name to check for timestamps (e.g., "last_modified_at")
    pub timestamp_property: String,

    /// Whether to fallback to Element.effective_from if the timestamp property is missing
    /// or is not an integer. Defaults to true.
    #[serde(default = "default_fallback")]
    pub fallback_to_effective_from: bool,
}

fn default_fallback() -> bool {
    true
}
