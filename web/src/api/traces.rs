use super::ApiClient;
use crate::utils::error::Result;
use probing_proto::prelude::{DataFrame, Ele, Process};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub record_type: String,
    pub trace_id: i64,
    pub span_id: i64,
    pub parent_id: Option<i64>,
    pub name: String,
    pub timestamp: i64,
    pub thread_id: i64,
    pub kind: Option<String>,
    pub location: Option<String>,
    pub attributes: Option<String>,
    pub event_attributes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanInfo {
    pub span_id: i64,
    pub trace_id: i64,
    pub parent_id: Option<i64>,
    pub name: String,
    pub start_timestamp: i64,
    pub end_timestamp: Option<i64>,
    pub thread_id: i64,
    pub kind: Option<String>,
    pub location: Option<String>,
    pub attributes: Option<String>,
    pub children: Vec<SpanInfo>,
    pub events: Vec<EventInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventInfo {
    pub name: String,
    pub timestamp: i64,
    pub attributes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceProcessInfo {
    pub pid: i32,
    pub process_role: Option<String>,
    pub hostname: Option<String>,
    pub ray_worker_id: Option<String>,
    pub ray_actor_id: Option<String>,
    pub ray_actor_name: Option<String>,
}

/// Tracing API
impl ApiClient {
    /// Get trace events, supports limiting count
    pub async fn get_trace_events(&self, limit: Option<usize>) -> Result<Vec<TraceEvent>> {
        let query = trace_events_query(limit);
        let df = self.execute_query(&query).await?;
        Ok(trace_events_from_df(df))
    }

    /// Get all trace events for spans tagged with a rollout_id.
    pub async fn get_trace_events_for_rollout_id(&self, rollout_id: &str) -> Result<Vec<TraceEvent>> {
        let span_ids = self.get_span_ids_for_rollout_id(rollout_id).await?;
        if span_ids.is_empty() {
            return Ok(Vec::new());
        }
        let query = trace_events_for_span_ids_query(&span_ids);
        let df = self.execute_query(&query).await?;
        Ok(trace_events_from_df(df))
    }

    async fn get_span_ids_for_rollout_id(&self, rollout_id: &str) -> Result<Vec<i64>> {
        let query = span_ids_for_rollout_query(rollout_id);
        let df = self.execute_query(&query).await?;
        Ok(span_ids_from_df(df))
    }

    /// Get trace events from another local probing process.
    pub async fn get_trace_events_for_pid(
        &self,
        pid: i32,
        limit: Option<usize>,
    ) -> Result<Vec<TraceEvent>> {
        let query = trace_events_query(limit);
        let df = self.execute_query_local_pid(pid, &query).await?;
        Ok(trace_events_from_df(df))
    }

    /// Get all rollout-tagged trace events from another local probing process.
    pub async fn get_trace_events_for_pid_and_rollout_id(
        &self,
        pid: i32,
        rollout_id: &str,
    ) -> Result<Vec<TraceEvent>> {
        let span_ids = self.get_span_ids_for_pid_and_rollout_id(pid, rollout_id).await?;
        if span_ids.is_empty() {
            return Ok(Vec::new());
        }
        let query = trace_events_for_span_ids_query(&span_ids);
        let df = self.execute_query_local_pid(pid, &query).await?;
        Ok(trace_events_from_df(df))
    }

    async fn get_span_ids_for_pid_and_rollout_id(&self, pid: i32, rollout_id: &str) -> Result<Vec<i64>> {
        let query = span_ids_for_rollout_query(rollout_id);
        let df = self.execute_query_local_pid(pid, &query).await?;
        Ok(span_ids_from_df(df))
    }

    /// Build span tree structure, supports limiting count
    pub async fn get_span_tree(&self, limit: Option<usize>) -> Result<Vec<SpanInfo>> {
        let events = self.get_trace_events(limit).await?;
        Ok(build_span_tree_from_events(events))
    }

    /// Build span tree for a single rollout_id without applying an event limit.
    pub async fn get_span_tree_for_rollout_id(&self, rollout_id: &str) -> Result<Vec<SpanInfo>> {
        let events = self.get_trace_events_for_rollout_id(rollout_id).await?;
        Ok(build_span_tree_from_events(events))
    }

    /// Build span tree from another local probing process.
    pub async fn get_span_tree_for_pid(
        &self,
        pid: i32,
        limit: Option<usize>,
    ) -> Result<Vec<SpanInfo>> {
        let events = self.get_trace_events_for_pid(pid, limit).await?;
        Ok(build_span_tree_from_events(events))
    }

    /// Build span tree for a single rollout_id from another local probing process.
    pub async fn get_span_tree_for_pid_and_rollout_id(
        &self,
        pid: i32,
        rollout_id: &str,
    ) -> Result<Vec<SpanInfo>> {
        let events = self.get_trace_events_for_pid_and_rollout_id(pid, rollout_id).await?;
        Ok(build_span_tree_from_events(events))
    }

    /// List Ray/probing processes known to the current driver process.
    pub async fn get_trace_processes(&self) -> Result<Vec<TraceProcessInfo>> {
        let query = r#"
            SELECT
                pid,
                hostname,
                ray_worker_id,
                ray_actor_id,
                ray_actor_name,
                process_role
            FROM python.ray_process
            WHERE pid > 0
            ORDER BY pid ASC
        "#;
        let df = self.execute_query(query).await?;
        let mut processes = trace_processes_from_df(df);
        if let Ok(local_processes) = self.get_local_probing_processes().await {
            merge_local_processes(&mut processes, local_processes);
        }
        processes.sort_by(|a, b| {
            process_sort_rank(a).cmp(&process_sort_rank(b))
                .then_with(|| a.pid.cmp(&b.pid))
        });
        Ok(processes)
    }

    /// List local processes exposing probing memtables.
    pub async fn get_local_probing_processes(&self) -> Result<Vec<Process>> {
        let response = self.get_request("/apis/processes/local").await?;
        Self::parse_json(&response)
    }

    /// Get JSON data in Chrome tracing format
    /// Returns format compatible with Chrome DevTools tracing viewer
    pub async fn get_chrome_tracing_json(&self, limit: Option<usize>) -> Result<String> {
        let mut events = self.get_trace_events(limit).await?;

        // Sort by timestamp ascending, ensure span_start is processed before span_end
        // This way when processing span_end, the corresponding span_start is already in span_starts
        events.sort_by_key(|e| e.timestamp);

        // Find minimum timestamp as baseline
        let min_timestamp = events.iter()
            .map(|e| e.timestamp)
            .min()
            .unwrap_or(0);

        // Convert to Chrome tracing format
        let mut trace_events: Vec<serde_json::Value> = Vec::new();

        // Use (process pid, span_id, thread_id) as key to avoid collisions when
        // spans from multiple Ray processes are exported together.
        let mut span_starts: std::collections::HashMap<(i64, i64, i64), (i64, String, Option<String>, i64)> = std::collections::HashMap::new();

        // First pass: collect all span_start events, build lookup table.
        // Include trace_id in the lookup because span_id is only unique inside a process.
        let mut span_start_lookup: std::collections::HashMap<(i64, i64, i64), (i64, String, Option<String>, i64)> = std::collections::HashMap::new();
        let mut span_process_lookup: std::collections::HashMap<(i64, i64, i64), i64> = std::collections::HashMap::new();
        let mut process_names: std::collections::HashMap<i64, String> = std::collections::HashMap::new();

        for event in &events {
            if event.record_type == "span_start" {
                let pid = trace_event_process_pid(event);
                span_process_lookup.insert((event.span_id, event.thread_id, event.trace_id), pid);
                let key = (pid, event.span_id, event.thread_id);
                span_start_lookup.insert(key, (
                    event.timestamp,
                    event.name.clone(),
                    event.kind.clone(),
                    pid,
                ));
                process_names.entry(pid).or_insert_with(|| {
                    process_label(&event.attributes, event.trace_id)
                });
            }
        }

        for (pid, name) in process_names.iter() {
            trace_events.push(serde_json::json!({
                "name": "process_name",
                "ph": "M",
                "pid": *pid,
                "tid": 0,
                "args": {
                    "name": name,
                },
            }));
        }

        // Second pass: convert events to Chrome tracing format
        for event in &events {
            // Convert nanoseconds to microseconds (Chrome tracing uses microseconds)
            let ts_micros = (event.timestamp - min_timestamp) / 1000;
            let pid = trace_event_process_pid_from_start(event, &span_process_lookup);
            let tid = event.thread_id as u32;

            match event.record_type.as_str() {
                "span_start" => {
                    let key = (pid, event.span_id, event.thread_id);
                    span_starts.insert(key, (ts_micros, event.name.clone(), event.kind.clone(), pid));

                    // Create 'B' (Begin) event
                    let mut chrome_event = serde_json::json!({
                        "name": event.name,
                        "cat": event.kind.as_ref().unwrap_or(&"span".to_string()),
                        "ph": "B",
                        "ts": ts_micros,
                        "pid": pid as u32,
                        "tid": tid,
                    });

                    // Add optional parameters
                    let mut args = serde_json::Map::new();
                    if let Some(ref location) = event.location {
                        if !location.is_empty() {
                            args.insert("location".to_string(), serde_json::Value::String(location.clone()));
                        }
                    }
                    if let Some(ref attrs) = event.attributes {
                        if !attrs.is_empty() {
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(attrs) {
                                args.insert("attributes".to_string(), parsed);
                            }
                        }
                    }
                    if !args.is_empty() {
                        chrome_event["args"] = serde_json::Value::Object(args);
                    }

                    trace_events.push(chrome_event);
                }
                "span_end" => {
                    let key = (pid, event.span_id, event.thread_id);

                    // First try to find from already processed events
                    if let Some((start_ts, start_name, start_kind, start_pid)) = span_starts.get(&key) {
                        // Found matching span_start, create 'E' (End) event
                        let mut chrome_event = serde_json::json!({
                            "name": start_name,
                            "cat": start_kind.as_ref().unwrap_or(&"span".to_string()),
                            "ph": "E",
                            "ts": ts_micros,
                            "pid": *start_pid as u32,
                            "tid": tid,
                        });

                        // Calculate duration (in microseconds)
                        let dur = ts_micros - start_ts;
                        if dur > 0 {
                            chrome_event["dur"] = serde_json::Value::Number(dur.into());
                        }

                        trace_events.push(chrome_event);
                        // Remove from span_starts to avoid duplicate matching
                        span_starts.remove(&key);
                    } else if let Some((start_timestamp, start_name, start_kind, start_pid)) = span_start_lookup.get(&key) {
                        // Find span_start information from lookup table
                        let start_ts_micros = (start_timestamp - min_timestamp) / 1000;
                        let mut chrome_event = serde_json::json!({
                            "name": start_name,
                            "cat": start_kind.as_ref().unwrap_or(&"span".to_string()),
                            "ph": "E",
                            "ts": ts_micros,
                            "pid": *start_pid as u32,
                            "tid": tid,
                        });

                        // Calculate duration (in microseconds)
                        let dur = ts_micros - start_ts_micros;
                        if dur > 0 {
                            chrome_event["dur"] = serde_json::Value::Number(dur.into());
                        }

                        trace_events.push(chrome_event);
                    } else {
                        // No matching span_start found (may have been filtered by limit)
                        let chrome_event = serde_json::json!({
                            "name": if event.name.is_empty() { "unknown_span" } else { &event.name },
                            "cat": "span",
                            "ph": "E",
                            "ts": ts_micros,
                            "pid": pid as u32,
                            "tid": tid,
                        });
                        trace_events.push(chrome_event);
                    }
                }
                "event" => {
                    // Create 'i' (Instant) event
                    let mut chrome_event = serde_json::json!({
                        "name": event.name,
                        "cat": "event",
                        "ph": "i",
                        "ts": ts_micros,
                        "pid": pid as u32,
                        "tid": tid,
                        "s": "t", // scope: thread
                    });

                    // Add event attributes
                    if let Some(ref attrs) = event.event_attributes {
                        if !attrs.is_empty() {
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(attrs) {
                                chrome_event["args"] = parsed;
                            }
                        }
                    }

                    trace_events.push(chrome_event);
                }
                _ => {}
            }
        }

        // Build complete Chrome tracing format JSON
        let chrome_trace = serde_json::json!({
            "traceEvents": trace_events,
            "displayTimeUnit": "ms",
        });

        Ok(serde_json::to_string_pretty(&chrome_trace)?)
    }

    /// Get Ray task execution timeline
    #[allow(dead_code)]
    pub async fn get_ray_timeline(
        &self,
        task_filter: Option<&str>,
        actor_filter: Option<&str>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<RayTimelineEntry>> {
        let mut query_params = Vec::new();

        if let Some(filter) = task_filter {
            query_params.push(format!("task_filter={}", urlencoding::encode(filter)));
        }
        if let Some(filter) = actor_filter {
            query_params.push(format!("actor_filter={}", urlencoding::encode(filter)));
        }
        if let Some(time) = start_time {
            query_params.push(format!("start_time={}", time));
        }
        if let Some(time) = end_time {
            query_params.push(format!("end_time={}", time));
        }

        let query_string = if query_params.is_empty() {
            String::new()
        } else {
            format!("?{}", query_params.join("&"))
        };

        let path = format!("/apis/python/ray/timeline{}", query_string);
        let response = self.get_request(&path).await?;
        Self::parse_json(&response)
    }

    /// Get Ray timeline in Chrome tracing format (for Perfetto UI)
    pub async fn get_ray_timeline_chrome_format(
        &self,
        task_filter: Option<&str>,
        actor_filter: Option<&str>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<String> {
        let mut query_params = Vec::new();

        if let Some(filter) = task_filter {
            query_params.push(format!("task_filter={}", urlencoding::encode(filter)));
        }
        if let Some(filter) = actor_filter {
            query_params.push(format!("actor_filter={}", urlencoding::encode(filter)));
        }
        if let Some(time) = start_time {
            query_params.push(format!("start_time={}", time));
        }
        if let Some(time) = end_time {
            query_params.push(format!("end_time={}", time));
        }

        let query_string = if query_params.is_empty() {
            String::new()
        } else {
            format!("?{}", query_params.join("&"))
        };

        let path = format!("/apis/python/ray/timeline/chrome{}", query_string);
        let response = self.get_request(&path).await?;

        // Check for error in the response JSON
        let json_value: serde_json::Value = serde_json::from_str(&response)?;
        if let Some(error_obj) = json_value.get("error") {
            return Err(crate::utils::error::AppError::Api(format!(
                "Backend error: {}",
                error_obj
            )));
        }

        Ok(response)
    }
}

fn trace_events_query(limit: Option<usize>) -> String {
        let limit_clause = if let Some(limit) = limit {
            format!("LIMIT {}", limit)
        } else {
            String::new()
        };

        format!(
            r#"
            SELECT
                record_type,
                trace_id,
                span_id,
                COALESCE(parent_id, -1) as parent_id,
                name,
                time as timestamp,
                COALESCE(thread_id, 0) as thread_id,
                kind,
                location,
                attributes,
                event_attributes
            FROM python.trace_event
            ORDER BY timestamp DESC
            {}
        "#,
            limit_clause
        )
}

fn span_ids_for_rollout_query(rollout_id: &str) -> String {
    let compact_numeric_comma = sql_string_literal(&format!("%\"rollout_id\":{},%", rollout_id));
    let compact_numeric_end = sql_string_literal(&format!("%\"rollout_id\":{}}}%", rollout_id));
    let spaced_numeric_comma = sql_string_literal(&format!("%\"rollout_id\": {},%", rollout_id));
    let spaced_numeric_end = sql_string_literal(&format!("%\"rollout_id\": {}}}%", rollout_id));
    let compact_string_comma = sql_string_literal(&format!("%\"rollout_id\":\"{}\",%", rollout_id));
    let compact_string_end = sql_string_literal(&format!("%\"rollout_id\":\"{}\"}}%", rollout_id));
    let spaced_string_comma = sql_string_literal(&format!("%\"rollout_id\": \"{}\",%", rollout_id));
    let spaced_string_end = sql_string_literal(&format!("%\"rollout_id\": \"{}\"}}%", rollout_id));

    format!(
        r#"
            SELECT DISTINCT span_id
            FROM python.trace_event
            WHERE record_type = 'span_start'
              AND (
                attributes LIKE {compact_numeric_comma}
                OR attributes LIKE {compact_numeric_end}
                OR attributes LIKE {spaced_numeric_comma}
                OR attributes LIKE {spaced_numeric_end}
                OR attributes LIKE {compact_string_comma}
                OR attributes LIKE {compact_string_end}
                OR attributes LIKE {spaced_string_comma}
                OR attributes LIKE {spaced_string_end}
              )
            ORDER BY span_id ASC
        "#
    )
}

fn trace_events_for_span_ids_query(span_ids: &[i64]) -> String {
    let span_ids = span_ids
        .iter()
        .map(|span_id| span_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"
            SELECT
                record_type,
                trace_id,
                span_id,
                COALESCE(parent_id, -1) as parent_id,
                name,
                time as timestamp,
                COALESCE(thread_id, 0) as thread_id,
                kind,
                location,
                attributes,
                event_attributes
            FROM python.trace_event
            WHERE span_id IN ({span_ids})
            ORDER BY timestamp DESC
        "#
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn span_ids_from_df(df: DataFrame) -> Vec<i64> {
    if df.names.is_empty() || df.cols.is_empty() {
        return Vec::new();
    }

    let span_id_idx = df.names.iter().position(|c| c == "span_id").unwrap_or(0);
    let nrows = df.cols.iter().map(|col| col.len()).max().unwrap_or(0);
    (0..nrows)
        .filter_map(|row_idx| match df.cols.get(span_id_idx).map(|col| col.get(row_idx)) {
            Some(Ele::I64(value)) => Some(value),
            Some(Ele::I32(value)) => Some(value as i64),
            Some(Ele::F32(value)) => Some(value as i64),
            Some(Ele::F64(value)) => Some(value as i64),
            Some(Ele::Text(value)) | Some(Ele::Url(value)) => value.parse::<i64>().ok(),
            Some(Ele::DataTime(value)) => i64::try_from(value).ok(),
            _ => None,
        })
        .collect()
}

fn trace_events_from_df(df: DataFrame) -> Vec<TraceEvent> {
    let mut events = Vec::new();

    if df.names.is_empty() || df.cols.is_empty() {
        return events;
    }

    // Find column indices
    let record_type_idx = df.names.iter().position(|c| c == "record_type").unwrap_or(0);
    let trace_id_idx = df.names.iter().position(|c| c == "trace_id").unwrap_or(1);
    let span_id_idx = df.names.iter().position(|c| c == "span_id").unwrap_or(2);
    let parent_id_idx = df.names.iter().position(|c| c == "parent_id").unwrap_or(3);
    let name_idx = df.names.iter().position(|c| c == "name").unwrap_or(4);
    let timestamp_idx = df.names.iter().position(|c| c == "timestamp").unwrap_or(5);
    let thread_id_idx = df.names.iter().position(|c| c == "thread_id").unwrap_or(6);
    let kind_idx = df.names.iter().position(|c| c == "kind").unwrap_or(7);
    let location_idx = df.names.iter().position(|c| c == "location").unwrap_or(8);
    let attributes_idx = df.names.iter().position(|c| c == "attributes").unwrap_or(9);
    let event_attributes_idx = df
        .names
        .iter()
        .position(|c| c == "event_attributes")
        .unwrap_or(10);

    // Get number of rows
    let nrows = df.cols.iter().map(|col| col.len()).max().unwrap_or(0);

    for row_idx in 0..nrows {
        let get_str = |idx: usize| -> String {
            match df.cols.get(idx).map(|col| col.get(row_idx)) {
                Some(Ele::Text(s)) => s.clone(),
                Some(Ele::I32(x)) => x.to_string(),
                Some(Ele::I64(x)) => x.to_string(),
                Some(Ele::F32(x)) => x.to_string(),
                Some(Ele::F64(x)) => x.to_string(),
                _ => "".to_string(),
            }
        };

        let get_i64 = |idx: usize| -> i64 {
            match df.cols.get(idx).map(|col| col.get(row_idx)) {
                Some(Ele::I32(x)) => x as i64,
                Some(Ele::I64(x)) => x,
                Some(Ele::F32(x)) => x as i64,
                Some(Ele::F64(x)) => x as i64,
                Some(Ele::Text(s)) => s.parse().unwrap_or(0),
                _ => 0,
            }
        };

        let get_opt_str = |idx: usize| -> Option<String> {
            match df.cols.get(idx).map(|col| col.get(row_idx)) {
                Some(Ele::Text(s)) if !s.is_empty() => Some(s.clone()),
                _ => None,
            }
        };

        let get_opt_i64 = |idx: usize| -> Option<i64> {
            let val = get_i64(idx);
            if val == -1 {
                None
            } else {
                Some(val)
            }
        };

        events.push(TraceEvent {
            record_type: get_str(record_type_idx),
            trace_id: get_i64(trace_id_idx),
            span_id: get_i64(span_id_idx),
            parent_id: get_opt_i64(parent_id_idx),
            name: get_str(name_idx),
            timestamp: get_i64(timestamp_idx),
            thread_id: get_i64(thread_id_idx),
            kind: get_opt_str(kind_idx),
            location: get_opt_str(location_idx),
            attributes: get_opt_str(attributes_idx),
            event_attributes: get_opt_str(event_attributes_idx),
        });
    }

    events
}

fn trace_processes_from_df(df: DataFrame) -> Vec<TraceProcessInfo> {
    let mut processes = Vec::new();

    if df.names.is_empty() || df.cols.is_empty() {
        return processes;
    }

    let pid_idx = df.names.iter().position(|c| c == "pid").unwrap_or(0);
    let hostname_idx = df.names.iter().position(|c| c == "hostname").unwrap_or(1);
    let worker_idx = df.names.iter().position(|c| c == "ray_worker_id").unwrap_or(2);
    let actor_idx = df.names.iter().position(|c| c == "ray_actor_id").unwrap_or(3);
    let actor_name_idx = df.names.iter().position(|c| c == "ray_actor_name").unwrap_or(4);
    let role_idx = df.names.iter().position(|c| c == "process_role").unwrap_or(5);
    let nrows = df.cols.iter().map(|col| col.len()).max().unwrap_or(0);
    let mut seen = std::collections::HashSet::<i32>::new();

    for row_idx in 0..nrows {
        let get_str = |idx: usize| -> String {
            match df.cols.get(idx).map(|col| col.get(row_idx)) {
                Some(Ele::Text(s)) => s.clone(),
                Some(Ele::I32(x)) => x.to_string(),
                Some(Ele::I64(x)) => x.to_string(),
                Some(Ele::F32(x)) => x.to_string(),
                Some(Ele::F64(x)) => x.to_string(),
                _ => "".to_string(),
            }
        };

        let pid = get_str(pid_idx).parse::<i32>().unwrap_or(0);
        if pid <= 0 || !seen.insert(pid) {
            continue;
        }

        let opt = |value: String| {
            if value.is_empty() {
                None
            } else {
                Some(value)
            }
        };

        processes.push(TraceProcessInfo {
            pid,
            process_role: opt(get_str(role_idx)),
            hostname: opt(get_str(hostname_idx)),
            ray_worker_id: opt(get_str(worker_idx)),
            ray_actor_id: opt(get_str(actor_idx)),
            ray_actor_name: opt(get_str(actor_name_idx)),
        });
    }

    processes.sort_by(|a, b| {
        process_sort_rank(a).cmp(&process_sort_rank(b))
            .then_with(|| a.pid.cmp(&b.pid))
    });
    processes
}

fn merge_local_processes(processes: &mut Vec<TraceProcessInfo>, local_processes: Vec<Process>) {
    let mut known = processes
        .iter()
        .map(|process| process.pid)
        .collect::<std::collections::HashSet<_>>();
    for process in local_processes {
        if process.pid <= 0 || !known.insert(process.pid) {
            continue;
        }
        processes.push(TraceProcessInfo {
            pid: process.pid,
            process_role: Some(local_process_role(&process.cmd)),
            hostname: None,
            ray_worker_id: None,
            ray_actor_id: None,
            ray_actor_name: local_actor_name(&process.cmd),
        });
    }
}

fn local_process_role(cmd: &str) -> String {
    if cmd.contains("RolloutManager.generate") {
        "rollout_actor".to_string()
    } else if cmd.contains("MegatronTrainRayActor.train") {
        "train_actor".to_string()
    } else if cmd.contains("SGLangEngine") {
        "sglang_engine".to_string()
    } else if cmd.contains("JobSupervisor") {
        "ray_job_supervisor".to_string()
    } else if cmd.contains("Lock") {
        "ray_lock".to_string()
    } else if cmd.contains("train_async.py") {
        "driver".to_string()
    } else {
        "process".to_string()
    }
}

fn local_actor_name(cmd: &str) -> Option<String> {
    for marker in [
        "RolloutManager.generate",
        "MegatronTrainRayActor.train",
        "SGLangEngine",
        "JobSupervisor",
        "Lock",
    ] {
        if cmd.contains(marker) {
            return Some(format!("ray::{marker}"));
        }
    }
    None
}

fn process_sort_rank(process: &TraceProcessInfo) -> usize {
    match process.process_role.as_deref() {
        Some("driver") => 0,
        Some(role) if role.contains("driver") => 1,
        Some(role) if role.contains("rollout") => 2,
        Some(_) => 3,
        None => 4,
    }
}

fn build_span_tree_from_events(mut events: Vec<TraceEvent>) -> Vec<SpanInfo> {
        events.sort_by_key(|event| event.timestamp);

        // Build span map from span_start events
        let mut span_map: std::collections::HashMap<(i64, i64), SpanInfo> = std::collections::HashMap::new();
        let mut root_spans: Vec<(i64, i64)> = Vec::new();
        let mut span_process_lookup: std::collections::HashMap<(i64, i64, i64), i64> = std::collections::HashMap::new();

        for event in &events {
            if event.record_type == "span_start" {
                let process_pid = trace_event_process_pid(event);
                span_process_lookup.insert((event.span_id, event.thread_id, event.trace_id), process_pid);
                let span_key = (process_pid, event.span_id);
                let span = SpanInfo {
                    span_id: event.span_id,
                    trace_id: event.trace_id,
                    parent_id: event.parent_id,
                    name: event.name.clone(),
                    start_timestamp: event.timestamp,
                    end_timestamp: None,
                    thread_id: event.thread_id,
                    kind: event.kind.clone(),
                    location: event.location.clone(),
                    attributes: event.attributes.clone(),
                    children: Vec::new(),
                    events: Vec::new(),
                };

                if event.parent_id.is_none() || event.parent_id == Some(-1) {
                    root_spans.push(span_key);
                }

                span_map.insert(span_key, span);
            } else if event.record_type == "span_end" {
                let process_pid = trace_event_process_pid_from_start(event, &span_process_lookup);
                let span_key = (process_pid, event.span_id);
                if let Some(span) = span_map.get_mut(&span_key) {
                    span.end_timestamp = Some(event.timestamp);
                }
            } else if event.record_type == "event" {
                let process_pid = trace_event_process_pid_from_start(event, &span_process_lookup);
                let span_key = (process_pid, event.span_id);
                if let Some(span) = span_map.get_mut(&span_key) {
                    span.events.push(EventInfo {
                        name: event.name.clone(),
                        timestamp: event.timestamp,
                        attributes: event.event_attributes.clone(),
                    });
                }
            }
        }

        // Build tree structure - process from deepest to shallowest
        // Calculate depth for each span using iterative approach
        let mut depth_map: std::collections::HashMap<(i64, i64), usize> = std::collections::HashMap::new();

        // Initialize all root spans to depth 0
        for root_id in &root_spans {
            depth_map.insert(*root_id, 0);
        }

        // Iteratively calculate depths until no changes
        let mut changed = true;
        while changed {
            changed = false;
            for (span_key, span) in span_map.iter() {
                if depth_map.contains_key(span_key) {
                    continue; // Already calculated
                }

                if let Some(parent_id) = span.parent_id {
                    let parent_key = (span_key.0, parent_id);
                    if parent_id != -1 && depth_map.contains_key(&parent_key) {
                        let parent_depth = depth_map[&parent_key];
                        depth_map.insert(*span_key, parent_depth + 1);
                        changed = true;
                    }
                } else {
                    // Root span (should have been added already, but handle it)
                    depth_map.insert(*span_key, 0);
                    changed = true;
                }
            }
        }

        // Sort spans by depth (deepest first) so we process children before parents
        let mut spans_to_process: Vec<((i64, i64), usize)> = span_map.keys()
            .map(|&key| (key, depth_map.get(&key).copied().unwrap_or(0)))
            .collect();
        spans_to_process.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by depth descending

        // Process spans from deepest to shallowest
        // This ensures that when we add a child to its parent, the child's children
        // have already been added to the child
        for (span_key, _depth) in spans_to_process {
            let parent_key = span_map.get(&span_key)
                .and_then(|span| span.parent_id)
                .filter(|&pid| pid != -1)
                .map(|parent_id| (span_key.0, parent_id));

            if let Some(parent_key) = parent_key {
                // Remove child from map and add to parent
                if let Some(child) = span_map.remove(&span_key) {
                    if let Some(parent) = span_map.get_mut(&parent_key) {
                        parent.children.push(child);
                    } else {
                        // Parent not found (shouldn't happen if depth calculation is correct)
                        // Put child back as orphan
                        span_map.insert(span_key, child);
                    }
                }
            }
        }

        // Collect root spans
        let mut result = Vec::new();
        for root_id in root_spans {
            if let Some(span) = span_map.remove(&root_id) {
                result.push(span);
            }
        }

        // Add any remaining spans (orphans)
        for (_, span) in span_map {
            result.push(span);
        }

        // Sort by start timestamp
        result.sort_by_key(|s| s.start_timestamp);

        result
}

fn trace_event_process_pid(event: &TraceEvent) -> i64 {
    attr_string(&event.attributes, "pid")
        .and_then(|pid| pid.parse::<i64>().ok())
        .filter(|pid| *pid > 0)
        .unwrap_or(event.trace_id.max(1))
}

fn trace_event_process_pid_from_start(
    event: &TraceEvent,
    span_process_lookup: &std::collections::HashMap<(i64, i64, i64), i64>,
) -> i64 {
    if event.trace_id > 0 {
        if let Some(pid) = span_process_lookup
            .get(&(event.span_id, event.thread_id, event.trace_id))
            .copied()
        {
            return pid;
        }
    }

    span_process_lookup
        .iter()
        .find(|((span_id, thread_id, _), _)| *span_id == event.span_id && *thread_id == event.thread_id)
        .map(|(_, pid)| *pid)
        .or_else(|| {
            span_process_lookup
                .iter()
                .find(|((span_id, _, _), _)| *span_id == event.span_id)
                .map(|(_, pid)| *pid)
        })
        .unwrap_or_else(|| trace_event_process_pid(event))
}

fn process_label(attributes: &Option<String>, fallback_trace_id: i64) -> String {
    let role = attr_string(attributes, "process_role").unwrap_or_else(|| "process".to_string());
    let pid = attr_string(attributes, "pid");
    match pid {
        Some(pid) if !pid.is_empty() => format!("{role}:{pid}"),
        _ => format!("{role}:{fallback_trace_id}"),
    }
}

fn attr_string(attributes: &Option<String>, key: &str) -> Option<String> {
    let attrs = attributes.as_ref()?;
    let value = serde_json::from_str::<serde_json::Value>(attrs).ok()?;
    let raw = value.get(key)?;
    match raw {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RayTimelineEntry {
    pub name: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub duration: Option<i64>,
    pub trace_id: i64,
    pub span_id: i64,
    pub parent_id: Option<i64>,
    pub kind: Option<String>,
    pub thread_id: i64,
    pub attributes: Option<serde_json::Value>,
}
