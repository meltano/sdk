# Emit version messages from a full-table tap

A tap can emit a Singer `ACTIVATE_VERSION` message at the start of each
full-table stream. The SDK emits it after `SCHEMA` and before the first
`RECORD` or `BATCH` message. With record output, each `RECORD` carries the
version announced by `ACTIVATE_VERSION`. With batch output, the SDK still
emits `ACTIVATE_VERSION`, but the `BATCH` messages do not carry that version.

## Enable version messages

Set `emit_activate_version_messages` in the tap's config:

```json
{
  "emit_activate_version_messages": true
}
```

The setting is off by default. It applies to streams using `FULL_TABLE`
replication, not to streams using `INCREMENTAL` replication. When a tap advertises
the `ACTIVATE_VERSION` capability, the SDK includes the setting in its
configuration schema. A capability declaration does not turn the setting on.

The tap emits the message; the target decides whether and how to act on it.
Check the target's documentation before relying on version messages to change
how snapshots are loaded.
