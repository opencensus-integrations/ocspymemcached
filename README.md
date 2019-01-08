# ocspymemcached
OpenCensus instrumented Spymemcached wrapper for tracing and metrics

## Recorded metrics

Metric|Search suffix|Additional tags
---|---|---
Number of Calls|"net.spy.memcached/calls"|"method", "error", "status"
Latency in milliseconds|"net.spy.memcached/latency"|"method", "error", "status"
Lengths of keys and values|"net.spy.memcached/length"|"method", "error", "status", "type"
