# Record Metadata

The SDK can automatically generate `_sdc_` ("Singer Data Capture") metadata properties when
performing data loads in SDK-based targets.

If `add_record_metadata` is defined as
a config option by the developer, and if the user sets `add_record_metadata=True` within
their own configuration, the following columns will be automatically added to each record:

- `_sdc_extracted_at` - Timestamp indicating when the record was extracted the record from the source.
- `_sdc_received_at` - Timestamp indicating when the record was received by the target for loading.
- `_sdc_batched_at` - Timestamp indicating when the record's batch was initiated.
- `_sdc_deleted_at` - Passed from a Singer tap if DELETE events are able to be tracked. In general, this is populated when the tap is synced LOG_BASED replication. If not sent from the tap, this field will be null.
- `_sdc_sequence` - The epoch (milliseconds) that indicates the order in which the record was queued for loading.
- `_sdc_table_version` - Indicates the version of the table. This column is used to determine when to issue TRUNCATE commands during loading, where applicable.
