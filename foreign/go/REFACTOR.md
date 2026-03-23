# Go SDK Refactoring Plan

Verified issues in `foreign/go/`, ordered by priority. Each item is one PR-sized unit.

---

## 1. CRITICAL: `panic()` in message deserialization

**File:** `binary_serialization/binary_response_deserializer.go:160`

```go
payloadSlice, err = s2.Decode(nil, payloadSlice)
if err != nil {
    panic("iggy: failed to decode s2 payload: " + err.Error())
}
```

The function returns `(*PolledMessage, error)` but panics instead of using the error return. A corrupted or truncated compressed message crashes the entire client process.

**Fix:** Replace `panic(...)` with `return nil, fmt.Errorf("iggy: failed to decode s2 payload: %w", err)`.

---

## 2. CRITICAL: Double bug in `SendMessages.MarshalBinary()` compression

**File:** `internal/command/message.go:49-71`

Two bugs in one block:

```go
for i, message := range s.Messages {
    // ...
    s.Messages[i].Payload = s2.Encode(nil, message.Payload)  // Bug A: mutates caller's slice
    message.Header.PayloadLength = uint32(len(message.Payload))  // Bug B: writes to loop copy, silently discarded
}
```

**Bug A — Caller mutation:** `MarshalBinary()` replaces the caller's `Messages[i].Payload` with compressed bytes. Calling `MarshalBinary()` twice or reusing the messages afterward gives corrupted results. The `encoding.BinaryMarshaler` contract expects the receiver to be unchanged.

**Bug B — Dead write:** `message` is a value copy from `range`. Writing `message.Header.PayloadLength = ...` modifies the copy, not `s.Messages[i].Header`. The PayloadLength is never actually updated to reflect the compressed size. This means the serialized wire format has the **original uncompressed length** in the header but **compressed bytes** in the payload — a length mismatch the server may silently accept or reject depending on how it reads.

**Fix:** Work on local copies, never mutate `s.Messages`:

```go
func (s *SendMessages) MarshalBinary() ([]byte, error) {
    messages := make([]iggcon.IggyMessage, len(s.Messages))
    copy(messages, s.Messages)
    for i := range messages {
        msg := &messages[i]
        if len(msg.Payload) < 32 {
            continue
        }
        switch s.Compression {
        case iggcon.MESSAGE_COMPRESSION_S2:
            msg.Payload = s2.Encode(nil, msg.Payload)
        case iggcon.MESSAGE_COMPRESSION_S2_BETTER:
            msg.Payload = s2.EncodeBetter(nil, msg.Payload)
        case iggcon.MESSAGE_COMPRESSION_S2_BEST:
            msg.Payload = s2.EncodeBest(nil, msg.Payload)
        }
        msg.Header.PayloadLength = uint32(len(msg.Payload))
    }
    // ... rest uses `messages` instead of `s.Messages`
}
```

Also collapses the three duplicated switch cases into one.

---

## 3. HIGH: Migrate `binary_response_deserializer.go` to use `codec.Reader`

**File:** `binary_serialization/binary_response_deserializer.go` (609 lines)

The entire file uses raw `binary.LittleEndian.Uint*()` calls with manual `position` tracking and **zero bounds checking**. A short payload causes an unrecoverable panic (slice out of bounds), not a returned error.

Meanwhile, `internal/codec/reader.go` already provides a safe `Reader` with:
- Automatic bounds checking (sets `err` on overrun instead of panicking)
- `U8()`, `U16()`, `U32()`, `U64()` methods
- `U8LenStr()` for the length-prefixed string pattern used ~10 times in the deserializer
- `Remaining()` for loop conditions

**Example of current code (repeated ~20 times):**

```go
id := binary.LittleEndian.Uint32(payload[position : position+4])
createdAt := binary.LittleEndian.Uint64(payload[position+4 : position+12])
nameLength := int(payload[position+32])
name := string(payload[position+33 : position+33+nameLength])
readBytes := 4 + 8 + 4 + 8 + 8 + 1 + nameLength
```

**What it becomes with codec.Reader:**

```go
r := codec.NewReader(payload)
id := r.U32()
createdAt := r.U64()
// ... (other fields)
name := r.U8LenStr()
if err := r.Err(); err != nil {
    return nil, err
}
```

This also addresses the known TODO at line 81 (`//TODO there's a deserialization bug... with payload greater than 2 pow 16`) — the bug is likely an off-by-one or overflow in manual position arithmetic that the Reader would prevent.

---

## 4. HIGH: Near-identical `MarshalBinary()` in offset commands

**File:** `internal/command/offset.go`

Three structs — `StoreConsumerOffsetRequest`, `GetConsumerOffset`, `DeleteConsumerOffset` — have nearly identical `MarshalBinary()` implementations. The only difference is `StoreConsumerOffsetRequest` appends an 8-byte offset at the end.

All three do the exact same:
1. Marshal consumer, streamId, topicId
2. Allocate `len(consumer) + len(stream) + len(topic) + N` bytes (magic `13` or `5`)
3. Copy with manual position tracking

**Fix:** Extract a shared helper:

```go
func marshalConsumerStreamTopic(consumer Consumer, streamId, topicId Identifier, extra int) ([]byte, int, error) { ... }
```

Or better, use `codec.Writer`:

```go
w := codec.NewWriter()
w.Obj(&s.Consumer)
w.Obj(&s.StreamId)
w.Obj(&s.TopicId)
// StoreOffset adds: w.U8(hasPartition); w.U32(partition); w.U64(s.Offset)
// GetOffset adds:   w.U8(hasPartition); w.U32(partition)
return w.Bytes(), w.Err()
```

---

## 5. MEDIUM: `deserializePermissions()` is fragile and deeply nested

**File:** `binary_serialization/binary_response_deserializer.go:372-455`

84 lines with 3 levels of nesting, manual `index` tracking, and `for { ... if bytes[index] == 0 { break } }` loops. No bounds checking — a malformed permissions payload causes an out-of-bounds panic.

**Fix:** Rewrite using `codec.Reader`. The nested loop structure stays, but bounds safety and readability improve significantly.

---

## 6. MEDIUM: `IggyTcpClient` has 46 methods across 13 files

**Files:** `client/tcp/tcp_*.go`

Counted 46 methods on `IggyTcpClient` (36 public + 10 private), spanning:
- Connection lifecycle (connect, disconnect, shutdown, read, write)
- Session management (login, logout, PAT auth)
- Stream/topic/partition/consumer group CRUD
- Message send/poll
- User management, offset management, access tokens

This is a God object, but it's a **low-urgency** refactor since each method is small (delegates to `do(cmd)`) and the files are already logically split. Splitting into composed sub-managers would be a larger effort.

---

## 7. MEDIUM: `SCREAMING_SNAKE_CASE` constants

**Files:**
- `contracts/config.go:23-27` — `MESSAGE_COMPRESSION_NONE`, `MESSAGE_COMPRESSION_S2`, etc.
- `contracts/message_polling.go:28-33` — `POLLING_OFFSET`, `POLLING_TIMESTAMP`, etc.

Go convention is `CamelCase` for exported constants. Other constants in the codebase already follow this (e.g., `Tcp`, `Quic`, `Active`, `Inactive`).

**Fix:** Rename to `MessageCompressionNone`, `PollingOffset`, etc. This is a breaking API change, so coordinate with any downstream consumers.

---

## 8. MEDIUM: Missing `.golangci.yml`

No golangci-lint configuration file exists. The pre-commit hook runs `golangci-lint run` with defaults, which means different developers may get different results depending on their local golangci-lint version.

**Fix:** Add a `.golangci.yml` in `foreign/go/` that pins linter versions and enables relevant linters (e.g., `errcheck`, `govet`, `staticcheck`, `gosimple`, `unused`).

---

## 9. LOW: Version not wired into login request

**File:** `contracts/version.go:20-22`

```go
// TODO: Wire Version into binary_serialization/log_in_request_serializer.go
// to send the actual SDK version during login instead of an empty string.
const Version = "0.7.1-edge.1"
```

The SDK version is defined but never sent to the server during login.

---

## 10. LOW: Incomplete event system (3 TODOs)

**File:** `client/tcp/tcp_core.go`

- Line 399: `// TODO publish event disconnected`
- Line 461: `// TODO event pushing logic`
- Line 475: `// TODO push shutdown event`

The reconnection and shutdown logic works, but there's no way for the caller to be notified of connection state changes. Needs a callback or channel-based event API.

---

## 11. LOW: Heartbeat errors silently logged

**File:** `client/iggy_client.go:92`

```go
if err := cli.Ping(); err != nil {
    log.Printf("[WARN] heartbeat failed: %v", err)
}
```

Uses `log.Printf` (global logger) instead of surfacing errors to the caller. Should at minimum accept a configurable error handler or callback.

---

## 12. LOW: Zero test coverage on TCP client and deserializer

**Test files found:** 12 total across the Go SDK.
**Zero tests for:**
- `client/tcp/` (13 source files, 0 test files)
- `binary_serialization/binary_response_deserializer.go` (609 lines, 0 tests — only `stats_serializer_test.go` exists)

The `internal/codec/` package has good test coverage (3 test files). The `internal/command/` package has partial coverage (3 test files for 10 source files).

---

## Suggested PR order

| PR | Items | Scope |
|----|-------|-------|
| 1  | #1 (panic fix) | 1-line fix, standalone |
| 2  | #2 (MarshalBinary double bug) | ~30 lines, standalone |
| 3  | #3 (deserializer -> codec.Reader) | ~300 lines touched, biggest win |
| 4  | #4 (offset command dedup) | ~60 lines, use codec.Writer |
| 5  | #5 (permissions deserialize) | Included in #3 if done together |
| 6  | #7 (naming), #8 (golangci.yml), #9 (version TODO) | Cleanup batch |
