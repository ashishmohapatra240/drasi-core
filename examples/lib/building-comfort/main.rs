// Copyright 2025 The Drasi Authors.
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

//! # Building Comfort Dashboard Example
//!
//! This example demonstrates real-time building comfort monitoring with:
//!
//! - **SSE Reaction**: Streaming query results to browser via Server-Sent Events
//! - **Graph Relationships**: Queries that traverse Building -> Floor -> Room hierarchy
//! - **Real-time Dashboard**: Visual browser UI that updates in real-time
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │ HTTP Source │────▶│   Queries   │────▶│SSE Reaction │────▶│  Browser    │
//! │  (port 9000)│     │ (3 Cypher)  │     │ (port 8080) │     │  Dashboard  │
//! └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
//! ```
//!
//! ## Running
//!
//! ```bash
//! cargo run
//! # Then open http://localhost:8081 in your browser
//! ```
//!
//! ## Testing
//!
//! Use the change.http file with VS Code REST Client or curl:
//!
//! ```bash
//! curl -X POST http://localhost:9000/sources/sensors/events \
//!   -H "Content-Type: application/json" \
//!   -d '{"operation":"update","element":{"type":"node","id":"room_101","labels":["Room"],"properties":{"name":"Room 101","temp":80,"humidity":42,"co2":450}}}'
//! ```

use anyhow::Result;
use axum::Router;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::services::ServeDir;

use drasi_bootstrap_scriptfile::ScriptFileBootstrapProvider;
use drasi_lib::{DrasiLib, Query};
use drasi_reaction_sse::SseReaction;
use drasi_source_http::HttpSource;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging - set RUST_LOG=debug for more detail
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("╔════════════════════════════════════════════╗");
    log::info!("║   Building Comfort Dashboard Example       ║");
    log::info!("╚════════════════════════════════════════════╝");

    // =========================================================================
    // Step 1: Create Bootstrap Provider
    // =========================================================================
    // The ScriptFile bootstrap provider loads initial building/floor/room data
    // from a JSONL file. This data populates queries when they first start.

    let bootstrap_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bootstrap_data.jsonl");

    log::info!("Loading bootstrap data from: {}", bootstrap_path.display());

    let bootstrap_provider = ScriptFileBootstrapProvider::builder()
        .with_file(bootstrap_path.to_string_lossy().to_string())
        .build();

    // =========================================================================
    // Step 2: Create HTTP Source
    // =========================================================================
    // The HTTP source exposes endpoints to receive sensor updates.
    // Events are sent to: POST /sources/sensors/events

    let http_source = HttpSource::builder("sensors")
        .with_host("0.0.0.0")
        .with_port(9000)
        .with_adaptive_enabled(true)
        .with_adaptive_max_batch_size(100)
        .with_adaptive_min_batch_size(1)
        .with_adaptive_max_wait_ms(50)
        .with_adaptive_min_wait_ms(10)
        .with_bootstrap_provider(bootstrap_provider)
        .build()?;

    // =========================================================================
    // Step 3: Define Queries
    // =========================================================================
    // Three continuous queries monitor building comfort:
    // 1. room-comfort: All rooms with calculated comfort levels
    // 2. room-alerts: Rooms outside acceptable range (< 40 or > 60)
    // 3. floor-comfort: Average comfort per floor (aggregation)
    //
    // Comfort formula: 50 + (temp - 72) + (humidity - 42)
    // Ideal comfort = 50, Acceptable range = 40-60

    // Query 1: Room Comfort - All rooms with comfort calculation
    let room_comfort_query = Query::cypher("room-comfort")
        .query(
            r#"
            MATCH (r:Room)-[:PART_OF]->(f:Floor)-[:PART_OF]->(b:Building)
            RETURN
                elementId(r) AS RoomId,
                r.name AS RoomName,
                elementId(f) AS FloorId,
                f.name AS FloorName,
                elementId(b) AS BuildingId,
                b.name AS BuildingName,
                r.temp AS Temperature,
                r.humidity AS Humidity,
                r.co2 AS CO2,
                (50 + (r.temp - 72) + (r.humidity - 42)) AS ComfortLevel
        "#,
        )
        .from_source("sensors")
        .auto_start(true)
        .enable_bootstrap(true)
        .build();

    // Query 2: Room Alerts - Rooms where comfort is outside acceptable range
    let room_alerts_query = Query::cypher("room-alerts")
        .query(
            r#"
            MATCH (r:Room)
            WITH
                elementId(r) AS RoomId,
                r.name AS RoomName,
                (50 + (r.temp - 72) + (r.humidity - 42)) AS ComfortLevel
            WHERE ComfortLevel < 40 OR ComfortLevel > 60
            RETURN RoomId, RoomName, ComfortLevel
        "#,
        )
        .from_source("sensors")
        .auto_start(true)
        .enable_bootstrap(true)
        .build();

    // Query 3: Floor Comfort - Average comfort per floor (aggregation)
    // This query demonstrates relationship traversal with aggregation
    let floor_comfort_query = Query::cypher("floor-comfort")
        .query(
            r#"
            MATCH (r:Room)-[:PART_OF]->(f:Floor)
            WITH
                f,
                (50 + (r.temp - 72) + (r.humidity - 42)) AS RoomComfortLevel
            RETURN
                elementId(f) AS FloorId,
                f.name AS FloorName,
                avg(RoomComfortLevel) AS ComfortLevel
        "#,
        )
        .from_source("sensors")
        .auto_start(true)
        .enable_bootstrap(true)
        .build();

    // =========================================================================
    // Step 4: Create SSE Reaction
    // =========================================================================
    // The SSE reaction streams query results to browser clients via Server-Sent
    // Events. All three queries stream to the same /events endpoint.

    let sse_reaction = SseReaction::builder("comfort-sse")
        .with_query("room-comfort")
        .with_query("room-alerts")
        .with_query("floor-comfort")
        .with_host("0.0.0.0")
        .with_port(8080)
        .with_sse_path("/events")
        .build()?;

    // =========================================================================
    // Step 5: Build DrasiLib
    // =========================================================================
    // Assemble all components into a DrasiLib instance.

    let core = Arc::new(
        DrasiLib::builder()
            .with_id("building-comfort")
            .with_source(http_source)
            .with_query(room_comfort_query)
            .with_query(room_alerts_query)
            .with_query(floor_comfort_query)
            .with_reaction(sse_reaction)
            .build()
            .await?,
    );

    // =========================================================================
    // Step 6: Start Processing
    // =========================================================================
    // Starting the core initializes all components:
    // 1. Sources start listening for events
    // 2. Queries subscribe to sources and run bootstrap
    // 3. Reactions auto-start and begin streaming

    core.start().await?;

    // =========================================================================
    // Step 7: Start Static File Server
    // =========================================================================
    // Serve the HTML dashboard on port 8081

    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    let static_service = Router::new().nest_service("/", ServeDir::new(&static_dir));

    let static_handle = tokio::spawn(async move {
        match tokio::net::TcpListener::bind("0.0.0.0:8081").await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, static_service).await {
                    log::error!("Static file server error: {e}");
                }
            }
            Err(e) => {
                log::error!("Failed to bind static file server: {e}");
            }
        }
    });

    log::info!("");
    log::info!("┌────────────────────────────────────────────┐");
    log::info!("│ Building Comfort Dashboard Started!        │");
    log::info!("├────────────────────────────────────────────┤");
    log::info!("│ Dashboard:    http://localhost:8081        │");
    log::info!("├────────────────────────────────────────────┤");
    log::info!("│ HTTP Source:  http://localhost:9000        │");
    log::info!("│   POST /sources/sensors/events             │");
    log::info!("│   POST /sources/sensors/events/batch       │");
    log::info!("│   GET  /health                             │");
    log::info!("├────────────────────────────────────────────┤");
    log::info!("│ SSE Stream:   http://localhost:8080/events │");
    log::info!("├────────────────────────────────────────────┤");
    log::info!("│ Queries:                                   │");
    log::info!("│   • room-comfort  - All rooms + comfort    │");
    log::info!("│   • room-alerts   - Rooms outside 40-60    │");
    log::info!("│   • floor-comfort - Avg comfort per floor  │");
    log::info!("├────────────────────────────────────────────┤");
    log::info!("│ Press Ctrl+C to stop                       │");
    log::info!("└────────────────────────────────────────────┘");
    log::info!("");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;

    log::info!("Shutting down gracefully...");
    static_handle.abort();
    core.stop().await?;
    log::info!("Shutdown complete.");

    Ok(())
}
