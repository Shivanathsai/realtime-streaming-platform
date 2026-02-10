"""Prometheus metrics for observability."""

from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Producer metrics
events_produced = Counter("events_produced_total", "Total events produced", ["topic", "event_type"])
produce_errors = Counter("produce_errors_total", "Producer errors", ["topic"])
produce_latency = Histogram("produce_latency_seconds", "Producer latency", ["topic"])

# Consumer metrics
events_consumed = Counter("events_consumed_total", "Total events consumed", ["topic", "consumer_group"])
consume_errors = Counter("consume_errors_total", "Consumer errors", ["topic"])
consume_lag = Gauge("consumer_lag", "Consumer lag", ["topic", "partition"])
commit_latency = Histogram("commit_latency_seconds", "Commit offset latency")

# Processor metrics
events_processed = Counter("events_processed_total", "Events processed", ["processor"])
processing_latency = Histogram("processing_latency_seconds", "Processing latency", ["processor"],
                                buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
window_aggregations = Counter("window_aggregations_total", "Window aggregations computed", ["window_type"])
state_size = Gauge("processor_state_size", "Number of keys in state store", ["processor"])

# Alert metrics
alerts_generated = Counter("alerts_generated_total", "Alerts generated", ["severity", "pattern"])
alerts_active = Gauge("alerts_active", "Currently active alerts", ["severity"])

# Pipeline metrics
pipeline_throughput = Gauge("pipeline_throughput_eps", "Events per second throughput")
pipeline_e2e_latency = Histogram("pipeline_e2e_latency_seconds", "End-to-end latency",
                                  buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0])
deadletter_events = Counter("deadletter_events_total", "Events sent to dead letter queue", ["reason"])


def start_metrics_server(port: int = 8000):
    start_http_server(port)
