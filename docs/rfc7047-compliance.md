# RFC 7047 Compliance Matrix

This document tracks RFC 7047 compliance for this Rust OVSDB client library.

The purpose of this matrix is to make compliance measurable. A feature is not considered complete until it has an explicit row in this file and the referenced tests exist and pass.

Primary source: RFC 7047, "The Open vSwitch Database Management Protocol".

Secondary implementation reference: Open vSwitch OVSDB documentation.

RFC 7047 defines OVSDB's JSON-RPC protocol, schema format, database notation, transaction operations, notifications, locking, echo, and security/TLS guidance.

## Status values

| Status | Meaning |
|---|---|
| TODO | No test exists yet. |
| PARTIAL | Some behavior is tested, but required happy or bad paths are missing. |
| DONE | The listed behavior has passing happy and bad path coverage. |
| NOT_APPLICABLE | The client does not implement or expose this RFC feature. |

## Test layers

| Layer | Purpose |
|---|---|
| real-server | Tests against a real ovsdb-server. |
| custom-schema | Tests against a real ovsdb-server using tests/schemas/rfc7047_compliance.ovsschema. |
| raw-wire | Sends raw JSON-RPC over TcpStream to a real ovsdb-server. |
| fake-server | Uses a fake/adversarial server to send malformed responses and notifications. |
| property | Property/fuzz tests for encoders and decoders. |
| fault | Fault-injection tests for disconnects, server death, I/O errors, and resource errors. |
| docs | Documentation-only decision or explanation. |

A real ovsdb-server proves interoperability with a compliant implementation. It does not prove the client safely handles malformed server responses, wrong request IDs, partial frames, unexpected notifications, or fault cases. Those require raw-wire, fake-server, property, and fault tests.

## Scope rules

This matrix tracks RFC 7047 client compliance for features exposed by this library.

A row is client-relevant if one of these is true:

1. The client sends the method or operation.
2. The client decodes the response.
3. The client receives the notification.
4. The client exposes typed builders or models for the RFC value/schema notation.
5. The client is expected to survive malformed or unexpected server data.

Server-only behavior is marked NOT_APPLICABLE unless the client exposes logic that must understand it.

OVS extensions such as monitor_cond, monitor_cond_change, monitor_cond_since, update2, and update3 are not counted as RFC 7047 core compliance. They may be tracked in a separate extension matrix.

## Compliance matrix

| ID | RFC Section | Requirement | Client responsibility | Happy path test | Bad path test | Layer | Status | Notes |
|---|---|---|---:|---|---|---|---|---|
| JSON-001 | 3.1 | UTF-8 JSON messages are accepted. | yes | rfc7047_wire_real_server::raw_utf8_json_request_success | rfc7047_fake_server::client_rejects_invalid_utf8_response | raw-wire/fake-server | TODO | RFC uses JSON for wire format. |
| JSON-002 | 3.1 | Client does not panic on malformed JSON response. | yes | N/A | rfc7047_fake_server::client_rejects_malformed_json_response | fake-server | TODO | Requires fake server. |
| JSON-003 | 3.1 | Client rejects or safely handles null bytes in strings if validation is implemented. | yes | rfc7047_notation::string_without_null_byte_success | rfc7047_notation::string_with_null_byte_rejected | property/custom-schema | TODO | RFC says implementations should disallow null bytes in strings. |
| JSON-004 | 3.1 | Signed 64-bit integer limits are handled. | yes | rfc7047_notation::integer_i64_min_max_success | rfc7047_notation::integer_outside_i64_rejected | property/custom-schema | TODO | Needed for atom handling. |
| JSON-005 | 3.1 | Client handles JSON values split across TCP reads. | yes | rfc7047_wire_real_server::raw_one_request_split_across_writes | rfc7047_fake_server::client_handles_response_split_across_reads | raw-wire/fake-server | TODO | Stream framing behavior. |
| JSON-006 | 3.1 | Client handles multiple JSON messages in one TCP read. | yes | rfc7047_wire_real_server::raw_two_requests_in_one_write | rfc7047_fake_server::client_handles_multiple_responses_in_one_read | raw-wire/fake-server | TODO | Stream framing behavior. |
| SCHEMA-001 | 3.2 | get_schema parses database name, version, and tables. | yes | rfc7047_custom_schema_smoke::custom_schema_get_schema_parses_tables | rfc7047_fake_server::client_rejects_schema_missing_required_fields | custom-schema/fake-server | PARTIAL | Task 2 provides the happy path only. |
| SCHEMA-002 | 3.2 | Client tolerates optional cksum field. | yes | rfc7047_custom_schema::schema_cksum_accepted | rfc7047_fake_server::schema_bad_cksum_type_rejected | custom-schema/fake-server | TODO | cksum is optional metadata. |
| SCHEMA-003 | 3.2 | Table isRoot true and false are parsed. | yes | rfc7047_custom_schema::schema_is_root_parsed | rfc7047_fake_server::schema_bad_is_root_type_rejected | custom-schema/fake-server | PARTIAL | Root/non-root behavior is exercised by GC tests. |
| SCHEMA-004 | 3.2 | maxRows is parsed. | yes | rfc7047_custom_schema::schema_max_rows_parsed | rfc7047_fake_server::schema_bad_max_rows_rejected | custom-schema/fake-server | PARTIAL | Runtime maxRows behavior is exercised by deferred-constraint tests. |
| SCHEMA-005 | 3.2 | indexes are parsed. | yes | rfc7047_custom_schema::schema_indexes_parsed | rfc7047_fake_server::schema_bad_index_rejected | custom-schema/fake-server | PARTIAL | Runtime index behavior is exercised by deferred-constraint tests. |
| SCHEMA-006 | 3.2 | column mutable:false is parsed. | yes | rfc7047_custom_schema::schema_mutable_false_parsed | rfc7047_fake_server::schema_bad_mutable_type_rejected | custom-schema/fake-server | TODO | Needed for readonly tests. |
| SCHEMA-007 | 3.2 | column ephemeral:true is parsed. | yes | rfc7047_custom_schema::schema_ephemeral_parsed | rfc7047_fake_server::schema_bad_ephemeral_type_rejected | custom-schema/fake-server | TODO | Needed for schema coverage. |
| SCHEMA-008 | 3.2 | atomic base types are parsed. | yes | rfc7047_custom_schema::schema_atomic_types_parsed | rfc7047_fake_server::schema_unknown_atomic_type_rejected | custom-schema/fake-server | TODO | integer, real, boolean, string, uuid. |
| SCHEMA-009 | 3.2 | enum constraints are parsed. | yes | rfc7047_custom_schema::schema_enum_parsed | rfc7047_fake_server::schema_bad_enum_rejected | custom-schema/fake-server | TODO | Uses enum_s. |
| SCHEMA-010 | 3.2 | min/max constraints are parsed. | yes | rfc7047_custom_schema::schema_min_max_parsed | rfc7047_fake_server::schema_min_greater_than_max_rejected | custom-schema/fake-server | TODO | integer, real, string, set, map. |
| SCHEMA-011 | 3.2 | strong and weak refTable/refType are parsed. | yes | rfc7047_custom_schema::schema_refs_parsed | rfc7047_fake_server::schema_bad_ref_type_rejected | custom-schema/fake-server | PARTIAL | Strong and weak reference behavior is exercised by notation and deferred-constraint tests. |
| RPC-LIST-001 | 4.1.1 | list_dbs returns database names. | yes | rfc7047_rpc_real_server::list_dbs_success | rfc7047_wire_real_server::list_dbs_bad_params_raw | real-server/raw-wire | PARTIAL | Happy path covered. Bad raw params still Task 5. |
| RPC-SCHEMA-001 | 4.1.2 | get_schema returns schema for known database. | yes | rfc7047_rpc_real_server::get_schema_success | rfc7047_rpc_real_server::get_schema_unknown_database_returns_rpc_error | real-server | DONE | Known and unknown database covered. |
| RPC-TRANSACT-001 | 4.1.3 | transact executes zero or more operations. | yes | rfc7047_rpc_real_server::transact_empty_transaction_success | rfc7047_rpc_real_server::transact_unknown_database_returns_rpc_error | real-server | PARTIAL | Basic transaction only; operation-specific coverage later. |
| RPC-TRANSACT-002 | 4.1.3 | operation errors are returned inside transaction result, not JSON-RPC error. | yes | rfc7047_transactions::transact_operation_error_result_shape | rfc7047_faults::transaction_operation_error_preserves_error_details | fake-server/raw-wire | DONE | Operation error objects and their details are preserved separately from JSON-RPC errors. |
| RPC-CANCEL-001 | 4.1.4 | cancel is sent as a notification and receives no direct reply. | yes | rfc7047_cancel::cancel_notification_receives_no_direct_reply | rfc7047_cancel::cancel_with_bad_params_does_not_break_connection | raw-wire | DONE | Raw cancel notification has no direct reply; client remains usable. |
| RPC-CANCEL-002 | 4.1.4 | cancel outstanding wait returns canceled to original request. | yes | rfc7047_cancel::cancel_original_request_receives_canceled | rfc7047_cancel::cancel_unknown_request_does_not_break_connection | raw-wire | DONE | Client cancellation now completes the pending wait with a canceled error and ignores late server replies. |
| RPC-MONITOR-001 | 4.1.5 | monitor initial snapshot works. | yes | rfc7047_monitor::monitor_initial_snapshot | rfc7047_monitor::monitor_unknown_table_fails | real-server | PARTIAL | Initial snapshot is covered; a dedicated bad-path monitor setup test is still missing. |
| RPC-UPDATE-001 | 4.1.6 | update notification is decoded. | yes | rfc7047_monitor::monitor_insert_update_has_new_only | rfc7047_fake_server::client_rejects_bad_update_notification | real-server/fake-server | PARTIAL | Real update shape is covered; malformed notification coverage still belongs to fake-server tests. |
| RPC-MONITOR-CANCEL-001 | 4.1.7 | monitor_cancel succeeds and stops updates. | yes | rfc7047_monitor::monitor_cancel_success | rfc7047_monitor::monitor_cancel_unknown_monitor_fails | real-server | DONE | Cancel success and unknown monitor failure are covered. |
| RPC-LOCK-001 | 4.1.8 | lock succeeds when free. | yes | rfc7047_locks::lock_free_returns_true | rfc7047_locks::lock_same_lock_twice_without_unlock_fails_or_client_rejects | real-server | DONE | Free acquisition and duplicate-lock rejection covered. |
| RPC-STEAL-001 | 4.1.8 | steal succeeds and changes owner. | yes | rfc7047_locks::steal_succeeds | rfc7047_locks::steal_same_lock_twice_without_unlock_fails_or_client_rejects | real-server | DONE | Steal success and duplicate-steal rejection covered. |
| RPC-UNLOCK-001 | 4.1.8 | unlock releases owned lock. | yes | rfc7047_locks::unlock_owner_releases_lock | rfc7047_locks::unlock_without_prior_lock_or_steal_fails_or_client_rejects | real-server | DONE | Unlock success and local rejection without ownership covered. |
| RPC-LOCKED-001 | 4.1.9 | locked notification is decoded. | yes | rfc7047_locks::queued_waiter_receives_locked_notification | rfc7047_fake_server::client_rejects_bad_locked_notification | real-server/fake-server | PARTIAL | Real locked notification is covered; malformed fake-server handling is still future work. |
| RPC-STOLEN-001 | 4.1.10 | stolen notification is decoded. | yes | rfc7047_locks::stolen_owner_receives_stolen_notification | rfc7047_fake_server::client_rejects_bad_stolen_notification | real-server/fake-server | PARTIAL | Real stolen notification is covered; malformed fake-server handling is still future work. |
| RPC-ECHO-001 | 4.1.11 | client can send echo to server. | yes | rfc7047_rpc_real_server::echo_round_trip_string; rfc7047_rpc_real_server::echo_arbitrary_json_params | rfc7047_wire_real_server::echo_bad_params_raw | real-server/raw-wire | PARTIAL | Happy path covered. Raw bad params later. |
| RPC-ECHO-002 | 4.1.11 | client replies to server-initiated echo. | yes | rfc7047_fake_server::client_replies_to_server_initiated_echo | rfc7047_fake_server::client_rejects_server_echo_without_id | fake-server | TODO | Client must implement echo too. |
| WIRE-001 | 4 | Request/response id matching works. | yes | rfc7047_wire_real_server::raw_response_id_matches_request_id | rfc7047_fake_server::client_rejects_wrong_response_id | raw-wire/fake-server | TODO | |
| WIRE-002 | 4 | Successful response has result and error:null. | yes | rfc7047_wire_real_server::raw_success_has_result_and_error_null | rfc7047_fake_server::client_rejects_success_missing_error | raw-wire/fake-server | TODO | |
| WIRE-003 | 4 | Error response has result:null and error object. | yes | rfc7047_wire_real_server::raw_error_has_result_null_and_error_object | rfc7047_fake_server::client_rejects_error_missing_result | raw-wire/fake-server | TODO | |
| WIRE-004 | 4 | Unknown response id is rejected or reported. | yes | N/A | rfc7047_fake_server::client_rejects_response_with_unknown_id | fake-server | TODO | |
| WIRE-005 | 4 | Out-of-order responses are matched by id. | yes | rfc7047_fake_server::client_matches_out_of_order_responses_by_id | rfc7047_fake_server::client_rejects_duplicate_response_for_same_id | fake-server | TODO | |
| WIRE-006 | 4 | Connection close while request pending is reported. | yes | N/A | rfc7047_faults::fake_server_closes_before_response_returns_error | fake-server | DONE | Connection close and partial-frame failures are covered by the fault suite. |
| NOTATION-ATOM-001 | 5.1 | string atom round-trips. | yes | rfc7047_notation::atom_string_round_trip | N/A | custom-schema/property | PARTIAL | Happy path covered; dedicated bad-path coverage still needed. |
| NOTATION-ATOM-002 | 5.1 | integer atom round-trips. | yes | rfc7047_notation::atom_integer_round_trip | N/A | custom-schema/property | PARTIAL | Happy path covered; dedicated bad-path coverage still needed. |
| NOTATION-ATOM-003 | 5.1 | real atom round-trips. | yes | rfc7047_notation::atom_real_round_trip | N/A | custom-schema/property | PARTIAL | Happy path covered; dedicated bad-path coverage still needed. |
| NOTATION-ATOM-004 | 5.1 | boolean atom round-trips. | yes | rfc7047_notation::atom_boolean_round_trip | N/A | custom-schema/property | PARTIAL | Happy path covered; dedicated bad-path coverage still needed. |
| NOTATION-UUID-001 | 5.1 | UUID notation round-trips. | yes | rfc7047_notation::uuid_round_trip | rfc7047_notation::bad_uuid_rejected | custom-schema/property | DONE | UUID happy and bad path covered. |
| NOTATION-NAMED-UUID-001 | 5.1 | named UUID references work. | yes | rfc7047_notation::named_uuid_reference_round_trip; rfc7047_notation::named_uuid_reference_inside_set; rfc7047_notation::named_uuid_reference_inside_map | rfc7047_notation::unknown_named_uuid_rejected | custom-schema | DONE | Named UUIDs covered in row, set, and map contexts. |
| NOTATION-SET-001 | 5.1 | empty set round-trips. | yes | rfc7047_notation::empty_set_round_trip | rfc7047_notation::bad_set_rejected | custom-schema/property | PARTIAL | Representative bad set path covered, but more variants remain. |
| NOTATION-SET-002 | 5.1 | singleton set can be encoded as atom. | yes | rfc7047_notation::singleton_set_as_atom_round_trip | rfc7047_notation::bad_set_rejected | custom-schema/property | PARTIAL | Representative bad set path covered, but more variants remain. |
| NOTATION-SET-003 | 5.1 | explicit set round-trips. | yes | rfc7047_notation::explicit_set_round_trip | rfc7047_notation::bad_set_rejected | custom-schema/property | PARTIAL | Representative bad set path covered, but more variants remain. |
| NOTATION-MAP-001 | 5.1 | empty map round-trips. | yes | rfc7047_notation::empty_map_round_trip | rfc7047_notation::bad_map_rejected | custom-schema/property | PARTIAL | Representative bad map path covered, but more variants remain. |
| NOTATION-MAP-002 | 5.1 | map round-trips. | yes | rfc7047_notation::map_round_trip | rfc7047_notation::bad_map_rejected | custom-schema/property | PARTIAL | Representative bad map path covered, but more variants remain. |
| NOTATION-MAP-003 | 5.1 | JSON object is not accepted as OVSDB map notation. | yes | rfc7047_notation::map_round_trip; rfc7047_notation::empty_map_round_trip | rfc7047_notation::map_encoded_as_json_object_rejected | custom-schema/property | DONE | JSON object map syntax rejected and canonical map notation covered. |
| TX-INSERT-001 | 5.2.1 | insert succeeds and returns UUID. | yes | rfc7047_transactions::insert_all_scalar_types | rfc7047_transactions::insert_wrong_type_fails | custom-schema | DONE | Happy and bad path covered. |
| TX-INSERT-002 | 5.2.1 | insert handles uuid-name. | yes | rfc7047_transactions::insert_all_scalar_types | rfc7047_transactions::insert_wrong_type_fails | custom-schema | PARTIAL | Insert path exists, but explicit uuid-name assertions still need separate coverage. |
| TX-SELECT-001 | 5.2.2 | select returns rows. | yes | rfc7047_transactions::select_projected_columns | rfc7047_transactions::select_unknown_column_fails | custom-schema | DONE | Happy and bad path covered. |
| TX-UPDATE-001 | 5.2.3 | update returns count. | yes | rfc7047_transactions::update_one_row_returns_count | rfc7047_transactions::update_wrong_type_fails | custom-schema | DONE | Happy and bad path covered. |
| TX-UPDATE-002 | 5.2.3 | update respects mutable:false and readonly columns. | yes | rfc7047_transactions::update_mutable_false_column_fails | rfc7047_transactions::update_mutable_false_column_fails | custom-schema | DONE | Readonly update rejection covered. |
| TX-MUTATE-001 | 5.2.4 | arithmetic mutators work. | yes | rfc7047_transactions::mutate_integer_add; rfc7047_transactions::mutate_integer_subtract; rfc7047_transactions::mutate_integer_multiply; rfc7047_transactions::mutate_integer_divide; rfc7047_transactions::mutate_integer_modulo | rfc7047_transactions::mutate_divide_by_zero_domain_error | custom-schema | PARTIAL | Integer arithmetic covered; real-number and overflow variants remain. |
| TX-MUTATE-002 | 5.2.4 | set mutators work. | yes | rfc7047_transactions::mutate_set_insert_delete | N/A | custom-schema | PARTIAL | Representative set behavior covered; more RFC variants remain. |
| TX-MUTATE-003 | 5.2.4 | map mutators work. | yes | rfc7047_transactions::mutate_map_insert_delete | N/A | custom-schema | PARTIAL | Representative map behavior covered; more RFC variants remain. |
| TX-DELETE-001 | 5.2.5 | delete returns count. | yes | rfc7047_transactions::delete_one_row_returns_count | rfc7047_transactions::delete_zero_rows_returns_count_zero | custom-schema | DONE | Happy and zero-row path covered. |
| TX-WAIT-001 | 5.2.6 | wait succeeds when condition is true. | yes | rfc7047_transactions::wait_equal_already_true | rfc7047_transactions::wait_timeout_zero_fails | custom-schema | PARTIAL | Success and timeout representative paths covered. |
| TX-WAIT-002 | 5.2.6 | wait times out when condition is false. | yes | rfc7047_transactions::wait_not_equal_already_true | rfc7047_transactions::wait_timeout_zero_fails | custom-schema | PARTIAL | Success and timeout representative paths covered. |
| TX-COMMIT-001 | 5.2.7 | commit durable false succeeds. | yes | rfc7047_transactions::commit_false_success | N/A | custom-schema/raw-wire | PARTIAL | Commit success covered; raw error-shape checks remain. |
| TX-ABORT-001 | 5.2.8 | abort rolls back and returns aborted. | yes | rfc7047_transactions::abort_rolls_back_prior_insert | N/A | custom-schema/raw-wire | PARTIAL | Rollback covered; raw null-after-error shape remains. |
| TX-COMMENT-001 | 5.2.9 | comment succeeds. | yes | rfc7047_transactions::comment_success | N/A | custom-schema | PARTIAL | Happy path covered only. |
| TX-ASSERT-001 | 5.2.10 | assert succeeds when lock is owned. | yes | rfc7047_transactions::assert_with_owned_lock_succeeds | rfc7047_transactions::assert_without_lock_fails | real-server/custom-schema | DONE | Lock-owned success and failure without lock covered. |
| COND-001 | 5.1 / 5.2 | integer comparison conditions work. | yes | rfc7047_conditions::condition_integer_all_comparisons | rfc7047_conditions::condition_integer_wrong_type_fails | custom-schema | DONE | Happy and bad-path coverage exists. |
| COND-002 | 5.1 / 5.2 | real comparison conditions work. | yes | rfc7047_conditions::condition_real_all_comparisons | rfc7047_conditions::condition_real_wrong_type_fails | custom-schema | DONE | Happy and bad-path coverage exists. |
| COND-003 | 5.1 / 5.2 | boolean equality/includes/excludes work. | yes | rfc7047_conditions::condition_boolean_eq_ne_includes_excludes | rfc7047_conditions::condition_lt_on_boolean_fails | custom-schema | DONE | Happy and bad-path coverage exists. |
| COND-004 | 5.1 / 5.2 | string equality/includes/excludes work. | yes | rfc7047_conditions::condition_string_eq_ne_includes_excludes | rfc7047_conditions::condition_lt_on_string_fails | custom-schema | DONE | Happy and bad-path coverage exists. |
| COND-005 | 5.1 / 5.2 | UUID equality/includes/excludes work. | yes | rfc7047_conditions::condition_uuid_eq_ne_includes_excludes | rfc7047_conditions::condition_lt_on_uuid_fails | custom-schema | DONE | Happy and bad-path coverage exists. |
| COND-006 | 5.1 / 5.2 | set equality/includes/excludes work. | yes | rfc7047_conditions::condition_set_eq_ne_includes_excludes | rfc7047_conditions::condition_set_wrong_type_fails | custom-schema | PARTIAL | Representative happy and bad-path coverage exists; edge cases remain. |
| COND-007 | 5.1 / 5.2 | map equality/includes/excludes work. | yes | rfc7047_conditions::condition_map_eq_ne_includes_excludes | rfc7047_conditions::condition_map_wrong_type_fails | custom-schema | PARTIAL | Representative happy and bad-path coverage exists; edge cases remain. |
| COND-008 | 5.1 / 5.2 | invalid condition operator fails safely. | yes | N/A | rfc7047_conditions::condition_invalid_operator_fails | custom-schema | DONE | Bad-path coverage exists. |
| DEFERRED-001 | 3.2 / 5.2 | maxRows violation appears as commit-time extra error result. | yes | N/A | rfc7047_deferred_constraints::max_rows_violation_appends_extra_error_result | custom-schema/raw-wire | DONE | Raw JSON-RPC extra-error shape covered. |
| DEFERRED-002 | 3.2 / 5.2 | index violation appears as commit-time extra error result. | yes | N/A | rfc7047_deferred_constraints::index_violation_appends_extra_error_result | custom-schema/raw-wire | DONE | Raw JSON-RPC extra-error shape covered. |
| DEFERRED-003 | 3.2 / 5.2 | strong reference violation appears as commit-time extra error result. | yes | N/A | rfc7047_deferred_constraints::strong_ref_violation_appends_extra_error_result | custom-schema/raw-wire | DONE | Raw JSON-RPC extra-error shape covered. |
| DEFERRED-004 | 3.2 / 5.2 | deferred error rolls back all operations. | yes | rfc7047_deferred_constraints::deferred_error_rolls_back_all_operations | N/A | custom-schema/raw-wire | DONE | Rollback verified after commit-time failure. |
| DEFERRED-005 | 3.2 / 5.2 | weak reference to deleted row is removed. | yes | rfc7047_deferred_constraints::weak_ref_to_deleted_row_is_removed; rfc7047_deferred_constraints::weak_ref_inside_map_removes_pair | rfc7047_deferred_constraints::weak_ref_cleanup_constraint_failure | custom-schema | DONE | Scalar and map weak-reference cleanup covered. |
| DEFERRED-006 | 3.2 / 5.2 | non-root unreferenced rows are garbage-collected. | yes | rfc7047_deferred_constraints::non_root_unreferenced_row_is_garbage_collected; rfc7047_deferred_constraints::referenced_non_root_row_persists; rfc7047_deferred_constraints::max_rows_checked_after_gc; rfc7047_deferred_constraints::index_checked_after_gc | N/A | custom-schema/raw-wire | DONE | GC and post-GC constraint ordering covered. |
| MONITOR-001 | 4.1.5 | monitor initial snapshot works. | yes | rfc7047_monitor::monitor_initial_snapshot | rfc7047_monitor::monitor_unknown_table_fails | real-server | PARTIAL | Initial snapshot is covered; a dedicated bad-path monitor setup test is still missing. |
| MONITOR-002 | 4.1.5 | monitor selected columns only. | yes | rfc7047_monitor::monitor_selected_columns | rfc7047_monitor::monitor_unknown_column_fails | real-server | PARTIAL | Column projection is covered; the unknown-column bad-path test is still missing. |
| MONITOR-003 | 4.1.5 | insert select flag controls insert updates. | yes | rfc7047_monitor::monitor_insert_true_sends_insert_update | rfc7047_monitor::monitor_insert_false_suppresses_insert | real-server | DONE | Insert suppression covered. |
| MONITOR-004 | 4.1.5 | modify select flag controls modify updates. | yes | rfc7047_monitor::monitor_modify_true_sends_modify_update | rfc7047_monitor::monitor_modify_false_suppresses_modify | real-server | DONE | Modify suppression covered. |
| MONITOR-005 | 4.1.5 | delete select flag controls delete updates. | yes | rfc7047_monitor::monitor_delete_true_sends_delete_update | rfc7047_monitor::monitor_delete_false_suppresses_delete | real-server | DONE | Delete suppression covered. |
| MONITOR-006 | 4.1.5 | duplicate/overlapping monitor columns fail. | yes | rfc7047_monitor::monitor_multiple_non_overlapping_requests_success | rfc7047_monitor::monitor_duplicate_columns_fails | real-server | DONE | Non-overlapping and overlapping requests covered. |
| MONITOR-007 | 4.1.6 | update notification shape for insert/modify/delete is decoded. | yes | rfc7047_monitor::monitor_update_shapes | rfc7047_fake_server::client_rejects_bad_update_notification | real-server/fake-server | PARTIAL | Real update decoding covered; malformed notification coverage remains for fake-server tests. |
| MONITOR-008 | 4.1.7 | monitor_cancel stops updates. | yes | rfc7047_monitor::monitor_cancel_success | rfc7047_monitor::monitor_cancel_unknown_monitor_fails | real-server | DONE | Cancel and follow-up suppression covered. |
| LOCK-001 | 4.1.8 | lock returns true when free. | yes | rfc7047_locks::lock_free_returns_true | rfc7047_locks::lock_same_lock_twice_without_unlock_fails_or_client_rejects | real-server | DONE | Free acquisition and duplicate-lock rejection covered. |
| LOCK-002 | 4.1.8 | lock queues waiters and sends locked notification. | yes | rfc7047_locks::lock_held_returns_false_and_queues | rfc7047_fake_server::client_rejects_bad_locked_notification | real-server/fake-server | DONE | Queueing and notification delivery covered. |
| LOCK-003 | 4.1.8 | lock waiters are FIFO. | yes | rfc7047_locks::lock_waiters_are_fifo | N/A | real-server | DONE | FIFO waiter order covered. |
| LOCK-004 | 4.1.8 | steal succeeds and sends stolen notification. | yes | rfc7047_locks::steal_succeeds | rfc7047_fake_server::client_rejects_bad_stolen_notification | real-server/fake-server | DONE | Steal and stolen notification delivery covered. |
| LOCK-005 | 4.1.8 | unlock owner releases lock. | yes | rfc7047_locks::unlock_owner_releases_lock | rfc7047_locks::unlock_without_prior_lock_or_steal_fails_or_client_rejects | real-server | DONE | Unlock behavior covered. |
| LOCK-006 | 4.1.8 | disconnect owner releases lock. | yes | rfc7047_locks::disconnect_owner_releases_lock | rfc7047_locks::disconnect_queued_waiter_removes_wait | real-server | DONE | Owner and queued waiter disconnect behavior covered. |
| LOCK-007 | 5.2.10 | assert succeeds only for current owner. | yes | rfc7047_locks::assert_succeeds_only_for_current_lock_owner | rfc7047_locks::assert_fails_after_stolen_notification | real-server | DONE | Lock ownership assertions covered. |
| TLS-001 | 7 | TLS connection with valid CA succeeds. | yes | rfc7047_tls::tls_connect_valid_ca_success | rfc7047_tls::tls_wrong_ca_rejected | real-server | TODO | |
| TLS-002 | 7 | TLS hostname verification works. | yes | rfc7047_tls::tls_list_dbs_success | rfc7047_tls::tls_wrong_hostname_rejected | real-server | TODO | |
| TLS-003 | 7 | plain client to TLS listener fails cleanly. | yes | N/A | rfc7047_tls::plain_client_to_tls_listener_fails | real-server | TODO | |
| TLS-004 | 7 | TLS client to plain listener fails cleanly. | yes | N/A | rfc7047_tls::tls_client_to_plain_listener_fails | real-server | TODO | |
| PROP-001 | 5.1 | atom encoder/decoder round-trips valid atoms. | yes | rfc7047_property::prop_atom_round_trip | rfc7047_property::prop_invalid_atoms_rejected | property | TODO | |
| PROP-002 | 5.1 | set encoder/decoder round-trips valid sets. | yes | rfc7047_property::prop_set_round_trip | rfc7047_property::prop_invalid_sets_rejected | property | TODO | |
| PROP-003 | 5.1 | map encoder/decoder round-trips valid maps. | yes | rfc7047_property::prop_map_round_trip | rfc7047_property::prop_invalid_maps_rejected | property | TODO | |
| PROP-004 | 4 / 4.1 | response decoder never panics on arbitrary JSON. | yes | rfc7047_property::prop_response_decoder_never_panics | N/A | property | TODO | |
| PROP-005 | 4.1.6 / 4.1.9 / 4.1.10 | notification decoder never panics on arbitrary JSON. | yes | rfc7047_property::prop_notification_decoder_never_panics | N/A | property | TODO | |
| FAULT-001 | 4 | pending request reports connection close. | yes | N/A | rfc7047_faults::server_killed_while_request_pending | fault | PARTIAL | Destructive server-kill test exists but is ignored by default. |
| FAULT-002 | 4.1.6 | monitor stream reports connection close. | yes | N/A | rfc7047_faults::server_killed_while_monitor_active | fault | PARTIAL | Destructive server-kill test exists but is ignored by default. |
| FAULT-003 | 4 / 5.2 | client preserves I/O error string and details. | yes | rfc7047_faults::fake_server_returns_io_error_preserved | N/A | fault/fake-server | DONE | Deterministic fake-server coverage preserves error fields. |
| FAULT-004 | 4 / 5.2 | client preserves resources exhausted error string and details. | yes | rfc7047_faults::fake_server_returns_resources_exhausted_preserved | N/A | fault/fake-server | DONE | Deterministic fake-server coverage preserves error fields. |

## Notes for future test writers

1. Do not mark a row DONE unless the named tests exist and pass.
2. Real ovsdb-server tests are necessary but not sufficient for client compliance.
3. Fake-server tests are required for malformed server responses because real ovsdb-server normally behaves correctly.
4. Raw-wire tests are required for JSON-RPC envelope semantics.
5. Custom-schema tests are required because the Open_vSwitch schema does not cover every RFC type and constraint in isolation.
6. Property tests are required to prove decoders do not panic on unexpected JSON.
7. Fault tests may be ignored by default if they are slow, but they must exist.
8. OVS extensions must not be counted as RFC 7047 core compliance.
