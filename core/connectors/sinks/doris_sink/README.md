# Doris Sink

The Doris sink connector loads messages from Iggy into Apache Doris using Doris Stream Load.

## Configuration

- `url`: Doris FE HTTP endpoint (for example: `http://localhost:8030`)
- `database`: Target Doris database
- `table`: Target Doris table
- `username` / `password`: Optional basic auth credentials
- `batch_size`: Number of messages per stream-load request (default: 100)
- `include_metadata`: Wrap payload with Iggy metadata envelope (default: `true`)
- `include_checksum`: Include checksum in metadata (default: `false`)
- `include_origin_timestamp`: Include origin timestamp in metadata (default: `false`)
- `columns`: Optional Doris `columns` stream-load header
- `label_prefix`: Optional Doris stream-load label prefix
- `max_retries`, `retry_delay`, `retry_backoff_multiplier`, `max_retry_delay`: Retry settings

### Example

```toml
[plugin_config]
url = "http://localhost:8030"
database = "events"
table = "iggy_messages"
username = "root"
password = "******"
batch_size = 100
include_metadata = true
include_checksum = true
include_origin_timestamp = true
max_retries = 3
retry_delay = "1s"
retry_backoff_multiplier = 2
max_retry_delay = "30s"
```
