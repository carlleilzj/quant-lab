from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server

SERVICE_UP = Gauge("service_up", "Service up (1)", ["service"])
HEARTBEAT_TS = Gauge("heartbeat_ts", "Heartbeat unix ts", ["service"])

EVENTS_IN = Counter("events_in_total", "Events in", ["service", "subject"])
EVENTS_OUT = Counter("events_out_total", "Events out", ["service", "subject"])

ORDERS_APPROVED = Counter("orders_approved_total", "Orders approved", ["service"])
ORDERS_REJECTED = Counter("orders_rejected_total", "Orders rejected", ["service"])

ORDERS_SENT = Counter("orders_sent_total", "Orders sent (execution)", ["service"])
ORDERS_ACKED = Counter("orders_acked_total", "Orders acked (execution)", ["service"])


def start_metrics_server(port: int, service: str):
    start_http_server(port)
    SERVICE_UP.labels(service=service).set(1)
