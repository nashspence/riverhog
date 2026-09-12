# riverhog_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance:30a3727f59 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/8`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:riverhog-provenance` — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `distribution` | "riverhog-provenance" |
| `exports` | object (80 fields) |
| `module` | "riverhog_provenance" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6859b4d42e73e6b5d387f58412dd571bde2460c0b3094d401ceb1a4ac832d3b -->

```json
{
  "distribution": "riverhog-provenance",
  "exports": {
    "DEFAULT_OBSERVER_AGENT_ID": {
      "kind": "constant",
      "value": "urn:uuid:43dd8300-a6bd-5f58-9cfd-4d5f8ec9421b"
    },
    "DerivativeJournalSeed": {
      "fields": [
        {
          "default": "required",
          "name": "journal_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "recorded_by_agent_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "state_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "activity_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "current_entry_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "current_entry_json_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "previous_entry_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "previous_entry_json_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "next_sequence",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "signature": "(journal_id: 'str', recorded_by_agent_id: 'str', state_id: 'str', activity_id: 'str', current_entry_id: 'str', current_entry_json_sha256: 'str', previous_entry_id: 'str', previous_entry_json_sha256: 'str', next_sequence: 'int') -> None"
    },
    "ExternalStateReference": {
      "fields": [
        {
          "default": "required",
          "name": "journal_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "entry_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "entry_json_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "state_id",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(journal_id: 'str', entry_id: 'str', entry_json_sha256: 'str', state_id: 'str') -> None"
    },
    "FileProvenanceBinding": {
      "fields": [
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "status",
          "type": "\"Literal['captured', 'omitted']\""
        },
        {
          "default": "None",
          "name": "journal_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "current_state_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "omission_reason",
          "type": "'str | None'"
        }
      ],
      "kind": "class",
      "signature": "(path: 'str', bytes: 'int', sha256: 'str', status: \"Literal['captured', 'omitted']\", journal_id: 'str | None' = None, current_state_id: 'str | None' = None, omission_reason: 'str | None' = None) -> None"
    },
    "FileStateObserver": {
      "kind": "class",
      "members": {
        "observe": {
          "kind": "method",
          "signature": "(self, request: 'ObservationRequest') -> 'ObservationResult'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "FileStateObserverFactory": {
      "kind": "type-alias",
      "value": "collections.abc.Callable[[], riverhog_provenance.interface.FileStateObserver]"
    },
    "INSTALLATION_ID_FILENAME": {
      "kind": "constant",
      "value": "provenance-installation-id"
    },
    "IncrementalJournalEntry": {
      "fields": [
        {
          "default": "required",
          "name": "frame",
          "type": "'JournalFrame'"
        },
        {
          "default": "required",
          "name": "journal_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "primary_lineage_id",
          "type": "'str | None'"
        },
        {
          "default": "required",
          "name": "agents",
          "type": "'tuple[str, ...]'"
        },
        {
          "default": "required",
          "name": "events",
          "type": "'tuple[str, ...]'"
        },
        {
          "default": "required",
          "name": "states",
          "type": "'tuple[tuple[str, str], ...]'"
        },
        {
          "default": "required",
          "name": "entities",
          "type": "'tuple[tuple[str, str, str], ...]'"
        },
        {
          "default": "required",
          "name": "entity_counts",
          "type": "'tuple[tuple[str, int], ...]'"
        },
        {
          "default": "required",
          "name": "bindings",
          "type": "'tuple[tuple[str, str, str], ...]'"
        },
        {
          "default": "required",
          "name": "external_states",
          "type": "'tuple[ExternalStateReference, ...]'"
        }
      ],
      "kind": "class",
      "signature": "(frame: 'JournalFrame', journal_id: 'str', primary_lineage_id: 'str | None', agents: 'tuple[str, ...]', events: 'tuple[str, ...]', states: 'tuple[tuple[str, str], ...]', entities: 'tuple[tuple[str, str, str], ...]', entity_counts: 'tuple[tuple[str, int], ...]', bindings: 'tuple[tuple[str, str, str], ...]', external_states: 'tuple[ExternalStateReference, ...]') -> None"
    },
    "JournalFrame": {
      "fields": [
        {
          "default": "required",
          "name": "sequence",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "json_bytes",
          "type": "'bytes'"
        },
        {
          "default": "required",
          "name": "document",
          "type": "'JsonObject'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(sequence: 'int', json_bytes: 'bytes', document: 'JsonObject', sha256: 'str') -> None"
    },
    "JournalSummary": {
      "fields": [
        {
          "default": "required",
          "name": "journal_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "primary_lineage_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "frames",
          "type": "'tuple[JournalFrame, ...]'"
        },
        {
          "default": "required",
          "name": "entries",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "tail_frame",
          "type": "'JournalFrame'"
        },
        {
          "default": "required",
          "name": "journal_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "current_binding_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "current_state_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "current_path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "current_bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "current_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "agent_ids",
          "type": "'frozenset[str]'"
        },
        {
          "default": "required",
          "name": "external_states",
          "type": "'tuple[ExternalStateReference, ...]'"
        }
      ],
      "kind": "class",
      "members": {
        "tail": {
          "kind": "property",
          "signature": "(self) -> 'JournalFrame'"
        }
      },
      "signature": "(journal_id: 'str', primary_lineage_id: 'str', frames: 'tuple[JournalFrame, ...]', entries: 'int', tail_frame: 'JournalFrame', journal_sha256: 'str', current_binding_id: 'str', current_state_id: 'str', current_path: 'str', current_bytes: 'int', current_sha256: 'str', agent_ids: 'frozenset[str]', external_states: 'tuple[ExternalStateReference, ...]') -> None"
    },
    "LargeValueDisposition": {
      "kind": "class",
      "members": {
        "DIGEST_ONLY": "digest_only",
        "FAIL": "fail",
        "NOT_RETAINED": "not_retained"
      },
      "signature": "(*values)"
    },
    "NativeObservationError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ObservationPolicy": {
      "fields": [
        {
          "default": "True",
          "name": "strict_consistency",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "attempt_noatime",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "verify_path_binding",
          "type": "'bool'"
        },
        {
          "default": "False",
          "name": "second_content_hash",
          "type": "'bool'"
        },
        {
          "default": "8388608",
          "name": "hash_chunk_bytes",
          "type": "'int'"
        },
        {
          "default": "1048576",
          "name": "inline_native_value_bytes",
          "type": "'int'"
        },
        {
          "default": "268435456",
          "name": "maximum_native_value_bytes",
          "type": "'int'"
        },
        {
          "default": "<LargeValueDisposition.DIGEST_ONLY: 'digest_only'>",
          "name": "large_value_disposition",
          "type": "'LargeValueDisposition'"
        },
        {
          "default": "True",
          "name": "capture_xattrs",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "capture_acl",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "capture_file_flags",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "capture_sparse_map",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "capture_special_features",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "capture_native_stat",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "resolve_principals",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "include_access_time",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "include_hostname",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "include_effective_principal",
          "type": "'bool'"
        },
        {
          "default": "100000",
          "name": "maximum_sparse_extents",
          "type": "'int'"
        },
        {
          "default": "8388608",
          "name": "resource_fork_chunk_bytes",
          "type": "'int'"
        },
        {
          "default": "8388608",
          "name": "native_stream_chunk_bytes",
          "type": "'int'"
        },
        {
          "default": "10000",
          "name": "maximum_native_streams",
          "type": "'int'"
        },
        {
          "default": "False",
          "name": "capture_system_acl",
          "type": "'bool'"
        },
        {
          "default": "False",
          "name": "windows_allow_shared_write",
          "type": "'bool'"
        },
        {
          "default": "False",
          "name": "windows_allow_shared_delete",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "windows_follow_non_name_surrogate_reparse_points",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "windows_capture_usn",
          "type": "'bool'"
        },
        {
          "default": "True",
          "name": "windows_capture_object_id",
          "type": "'bool'"
        }
      ],
      "kind": "class",
      "signature": "(strict_consistency: 'bool' = True, attempt_noatime: 'bool' = True, verify_path_binding: 'bool' = True, second_content_hash: 'bool' = False, hash_chunk_bytes: 'int' = 8388608, inline_native_value_bytes: 'int' = 1048576, maximum_native_value_bytes: 'int' = 268435456, large_value_disposition: 'LargeValueDisposition' = <LargeValueDisposition.DIGEST_ONLY: 'digest_only'>, capture_xattrs: 'bool' = True, capture_acl: 'bool' = True, capture_file_flags: 'bool' = True, capture_sparse_map: 'bool' = True, capture_special_features: 'bool' = True, capture_native_stat: 'bool' = True, resolve_principals: 'bool' = True, include_access_time: 'bool' = True, include_hostname: 'bool' = True, include_effective_principal: 'bool' = True, maximum_sparse_extents: 'int' = 100000, resource_fork_chunk_bytes: 'int' = 8388608, native_stream_chunk_bytes: 'int' = 8388608, maximum_native_streams: 'int' = 10000, capture_system_acl: 'bool' = False, windows_allow_shared_write: 'bool' = False, windows_allow_shared_delete: 'bool' = False, windows_follow_non_name_surrogate_reparse_points: 'bool' = True, windows_capture_usn: 'bool' = True, windows_capture_object_id: 'bool' = True) -> None"
    },
    "ObservationRequest": {
      "fields": [
        {
          "default": "required",
          "name": "path",
          "type": "'PathInput'"
        },
        {
          "default": "required",
          "name": "lineage_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "host_id",
          "type": "'str'"
        },
        {
          "default": "None",
          "name": "host_entity_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "observer_agent_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "state_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "capture_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "environment_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "binding_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "payload_binding",
          "type": "'PayloadBindingRequest | None'"
        },
        {
          "default": "factory",
          "name": "policy",
          "type": "'ObservationPolicy'"
        },
        {
          "default": "()",
          "name": "additional_agents",
          "type": "'tuple[Mapping[str, Any], ...]'"
        },
        {
          "default": "()",
          "name": "additional_associations",
          "type": "'tuple[Mapping[str, Any], ...]'"
        },
        {
          "default": "()",
          "name": "notes",
          "type": "'tuple[str, ...]'"
        }
      ],
      "kind": "class",
      "signature": "(path: 'PathInput', lineage_id: 'str', host_id: 'str', host_entity_id: 'str | None' = None, observer_agent_id: 'str | None' = None, state_id: 'str | None' = None, capture_id: 'str | None' = None, environment_id: 'str | None' = None, binding_id: 'str | None' = None, payload_binding: 'PayloadBindingRequest | None' = None, policy: 'ObservationPolicy' = <factory>, additional_agents: 'tuple[Mapping[str, Any], ...]' = (), additional_associations: 'tuple[Mapping[str, Any], ...]' = (), notes: 'tuple[str, ...]' = ()) -> None"
    },
    "ObservationResult": {
      "fields": [
        {
          "default": "required",
          "name": "state",
          "type": "'JsonObject'"
        },
        {
          "default": "required",
          "name": "capture",
          "type": "'JsonObject'"
        },
        {
          "default": "required",
          "name": "environment",
          "type": "'JsonObject'"
        },
        {
          "default": "required",
          "name": "agents",
          "type": "'tuple[JsonObject, ...]'"
        },
        {
          "default": "()",
          "name": "payload_bindings",
          "type": "'tuple[JsonObject, ...]'"
        },
        {
          "default": "()",
          "name": "extensions",
          "type": "'tuple[JsonObject, ...]'"
        }
      ],
      "kind": "class",
      "members": {
        "assertion_body": {
          "kind": "method",
          "signature": "(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'"
        },
        "graph_fragment": {
          "kind": "method",
          "signature": "(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'"
        },
        "make_assertion_entry": {
          "kind": "method",
          "signature": "(self, *, journal_id: 'str', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', previous_sequence: 'int | None' = None, entry_id: 'str | None' = None, recorded_at: 'str | None' = None, recorded_by_agent_id: 'str | None' = None, notes: 'Sequence[str]' = (), omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'"
        },
        "payload_binding": {
          "kind": "property",
          "signature": "(self) -> 'JsonObject | None'"
        }
      },
      "signature": "(state: 'JsonObject', capture: 'JsonObject', environment: 'JsonObject', agents: 'tuple[JsonObject, ...]', payload_bindings: 'tuple[JsonObject, ...]' = (), extensions: 'tuple[JsonObject, ...]' = ()) -> None"
    },
    "PROVENANCE_BINDING_SEGMENT_BYTES_MAX": {
      "kind": "constant",
      "value": 4194304
    },
    "PROVENANCE_BINDING_SEGMENT_FILES_MAX": {
      "kind": "constant",
      "value": 512
    },
    "PROVENANCE_BINDING_SEGMENT_SCHEMA": {
      "kind": "constant",
      "value": "riverhog-provenance-bindings/v1"
    },
    "PROVENANCE_ENTRY_SCHEMA": {
      "kind": "constant",
      "value": "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"
    },
    "PROVENANCE_JOURNAL_ENTRY_BYTES_MAX": {
      "kind": "constant",
      "value": 8388608
    },
    "PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX": {
      "kind": "constant",
      "value": 8388608
    },
    "PROVENANCE_OBSERVER_BINDING_FORMAT": {
      "kind": "constant",
      "value": "riverhog-provenance-observer-binding/v1"
    },
    "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP": {
      "kind": "constant",
      "value": "riverhog.provenance-observers"
    },
    "PROVENANCE_OBSERVER_REFERENCE_FORMAT": {
      "kind": "constant",
      "value": "riverhog-provenance-observer-reference/v1"
    },
    "PROVENANCE_PROFILE": {
      "kind": "constant",
      "value": "https://nashspence.github.io/riverhog/v1/provenance"
    },
    "PROVENANCE_ROOT_DOCUMENT_BYTES_MAX": {
      "kind": "constant",
      "value": 65536
    },
    "PROVENANCE_ROOT_SCHEMA": {
      "kind": "constant",
      "value": "riverhog-provenance-root/v1"
    },
    "PROVENANCE_SEQUENCE_BITS": {
      "kind": "constant",
      "value": 256
    },
    "PROVENANCE_SEQUENCE_HEX_WIDTH": {
      "kind": "constant",
      "value": 64
    },
    "PROVENANCE_TERMINAL_SCHEMA": {
      "kind": "constant",
      "value": "riverhog-provenance-terminal/v1"
    },
    "PROVENANCE_VOLUME_DOCUMENT_BYTES_MAX": {
      "kind": "constant",
      "value": 65536
    },
    "PROVENANCE_VOLUME_SCHEMA": {
      "kind": "constant",
      "value": "riverhog-provenance-volume/v1"
    },
    "PayloadBindingRequest": {
      "fields": [
        {
          "default": "None",
          "name": "relative_path",
          "type": "'PathInput | None'"
        },
        {
          "default": "'co_resident_primary_payload'",
          "name": "role",
          "type": "'str'"
        },
        {
          "default": "None",
          "name": "replaces_binding_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "note",
          "type": "'str | None'"
        }
      ],
      "kind": "class",
      "signature": "(relative_path: 'PathInput | None' = None, role: 'str' = 'co_resident_primary_payload', replaces_binding_id: 'str | None' = None, note: 'str | None' = None) -> None"
    },
    "PreparedFileProvenance": {
      "fields": [
        {
          "default": "required",
          "name": "binding",
          "type": "'FileProvenanceBinding'"
        },
        {
          "default": "required",
          "name": "journals",
          "type": "'dict[str, bytes]'"
        },
        {
          "default": "required",
          "name": "source",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(binding: 'FileProvenanceBinding', journals: 'dict[str, bytes]', source: 'str') -> None"
    },
    "ProvenanceObserverBinding": {
      "fields": [
        {
          "default": "required",
          "name": "observer_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "contract_provider",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "contract_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "contract_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "factory",
          "type": "'FileStateObserverFactory'"
        },
        {
          "default": "'riverhog-provenance-observer-binding/v1'",
          "name": "format",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(observer_id: 'str', contract_provider: 'str', contract_id: 'str', contract_sha256: 'str', factory: 'FileStateObserverFactory', format: 'str' = 'riverhog-provenance-observer-binding/v1') -> None"
    },
    "ProvenanceObserverError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ProvenancePayloadIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "kind",
          "type": "\"Literal['bindings', 'journal']\""
        },
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(kind: \"Literal['bindings', 'journal']\", path: 'str', bytes: 'int', sha256: 'str') -> None"
    },
    "ProvenanceProviderMetadata": {
      "fields": [
        {
          "default": "required",
          "name": "name",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "value",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "distribution",
          "type": "'str | None'"
        },
        {
          "default": "required",
          "name": "version",
          "type": "'str | None'"
        }
      ],
      "kind": "class",
      "members": {
        "as_dict": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, str | None]'"
        }
      },
      "signature": "(name: 'str', value: 'str', distribution: 'str | None', version: 'str | None') -> None"
    },
    "ProvenanceRootDocument": {
      "fields": [
        {
          "default": "required",
          "name": "archive_generation",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "archive_tree_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "ordered_volume_sha256",
          "type": "'str'"
        },
        {
          "default": "'riverhog-provenance-root/v1'",
          "name": "schema",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, content: 'bytes') -> 'ProvenanceRootDocument'"
        },
        "identity": {
          "kind": "property",
          "signature": "(self) -> 'str'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(archive_generation: 'str', archive_tree_sha256: 'str', ordered_volume_sha256: 'str', schema: 'str' = 'riverhog-provenance-root/v1') -> None"
    },
    "ProvenanceTerminalDocument": {
      "fields": [
        {
          "default": "required",
          "name": "archive_generation",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "archive_tree_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "sequence",
          "type": "'int'"
        },
        {
          "default": "'terminal'",
          "name": "kind",
          "type": "\"Literal['terminal']\""
        },
        {
          "default": "'riverhog-provenance-terminal/v1'",
          "name": "schema",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, content: 'bytes') -> 'ProvenanceTerminalDocument'"
        },
        "metadata_path": {
          "kind": "property",
          "signature": "(self) -> 'str'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(archive_generation: 'str', archive_tree_sha256: 'str', sequence: 'int', kind: \"Literal['terminal']\" = 'terminal', schema: 'str' = 'riverhog-provenance-terminal/v1') -> None"
    },
    "ProvenanceValidationError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ProvenanceVolumeDocument": {
      "fields": [
        {
          "default": "required",
          "name": "archive_generation",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "archive_tree_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "sequence",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "payload",
          "type": "'ProvenancePayloadIdentity'"
        },
        {
          "default": "None",
          "name": "first_file_order",
          "type": "'int | None'"
        },
        {
          "default": "None",
          "name": "file_count",
          "type": "'int | None'"
        },
        {
          "default": "None",
          "name": "journal_id",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "journal_offset",
          "type": "'int | None'"
        },
        {
          "default": "None",
          "name": "journal_bytes",
          "type": "'int | None'"
        },
        {
          "default": "None",
          "name": "journal_sha256",
          "type": "'str | None'"
        },
        {
          "default": "'riverhog-provenance-volume/v1'",
          "name": "schema",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, content: 'bytes') -> 'ProvenanceVolumeDocument'"
        },
        "metadata_path": {
          "kind": "property",
          "signature": "(self) -> 'str'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(archive_generation: 'str', archive_tree_sha256: 'str', sequence: 'int', payload: 'ProvenancePayloadIdentity', first_file_order: 'int | None' = None, file_count: 'int | None' = None, journal_id: 'str | None' = None, journal_offset: 'int | None' = None, journal_bytes: 'int | None' = None, journal_sha256: 'str | None' = None, schema: 'str' = 'riverhog-provenance-volume/v1') -> None"
    },
    "ResolvedProvenanceObserver": {
      "fields": [
        {
          "default": "required",
          "name": "name",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "metadata",
          "type": "'ProvenanceProviderMetadata'"
        },
        {
          "default": "required",
          "name": "binding",
          "type": "'ProvenanceObserverBinding'"
        },
        {
          "default": "required",
          "name": "contract",
          "type": "'ProvenanceContractBinding'"
        },
        {
          "default": "required",
          "name": "_validator",
          "type": "'Callable[[Mapping[str, Any]], None]'"
        }
      ],
      "kind": "class",
      "members": {
        "as_dict": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        },
        "create": {
          "kind": "method",
          "signature": "(self) -> 'FileStateObserver'"
        },
        "observer_reference": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(name: 'str', metadata: 'ProvenanceProviderMetadata', binding: 'ProvenanceObserverBinding', contract: 'ProvenanceContractBinding', _validator: 'Callable[[Mapping[str, Any]], None]') -> None"
    },
    "SIDECAR_SUFFIX": {
      "kind": "constant",
      "value": ".riverhog-provenance.json-seq"
    },
    "SchemaValidationUnavailable": {
      "kind": "class",
      "signature": "unavailable"
    },
    "SymlinkRefusedError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "UnstableFileError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "UnsupportedFileTypeError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "UnsupportedPlatformError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "append_observation": {
      "kind": "function",
      "signature": "(content: 'bytes', path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver', policy: 'ObservationPolicy | None' = None) -> 'bytes'"
    },
    "append_replacement_transformation": {
      "kind": "function",
      "signature": "(content: 'bytes', output_path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', observer: 'FileStateObserver', evidence: 'Sequence[Mapping[str, Any]]' = (), policy: 'ObservationPolicy | None' = None) -> 'bytes'"
    },
    "binding_segment_bytes": {
      "kind": "function",
      "signature": "(*, first_file_order: 'int', files: 'list[Mapping[str, object]]') -> 'bytes'"
    },
    "bounded_binding_segment_bytes": {
      "kind": "function",
      "signature": "(*, first_file_order: 'int', files: 'list[Mapping[str, object]]') -> 'tuple[bytes, int]'"
    },
    "canonical_sidecar_path": {
      "kind": "function",
      "signature": "(payload: 'Path') -> 'Path'"
    },
    "create_derivative_journal": {
      "kind": "function",
      "signature": "(output_path: 'Path', *, relative_path: 'str', source_journals: 'Sequence[bytes]', host_id: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', observer: 'FileStateObserver', derivation_kind: 'str' = 'transformation', evidence: 'Sequence[Mapping[str, Any]]' = (), policy: 'ObservationPolicy | None' = None) -> 'bytes'"
    },
    "create_derivative_journal_from_identity": {
      "kind": "function",
      "signature": "(*, relative_path: 'str', byte_count: 'int', sha256: 'str', source_journals: 'Sequence[bytes]', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', journal_id: 'str | None' = None, derivation_kind: 'str' = 'transformation', evidence: 'Sequence[Mapping[str, Any]]' = ()) -> 'bytes'"
    },
    "create_derivative_journal_seed": {
      "kind": "function",
      "signature": "(*, relative_path: 'str', byte_count: 'int', sha256: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', journal_id: 'str') -> 'tuple[bytes, DerivativeJournalSeed]'"
    },
    "create_derivative_source_entry": {
      "kind": "function",
      "signature": "(*, seed: 'DerivativeJournalSeed', references: 'Sequence[ExternalStateReference]', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', recorded_at: 'str') -> 'bytes'"
    },
    "create_observation_journal": {
      "kind": "function",
      "signature": "(path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver', policy: 'ObservationPolicy | None' = None) -> 'bytes'"
    },
    "current_state_reference": {
      "kind": "function",
      "signature": "(content: 'bytes') -> 'ExternalStateReference'"
    },
    "format_provenance_sequence": {
      "kind": "function",
      "signature": "(value: 'int') -> 'str'"
    },
    "list_provenance_observers": {
      "kind": "function",
      "signature": "() -> 'tuple[ProvenanceProviderMetadata, ...]'"
    },
    "load_or_create_installation_id": {
      "kind": "function",
      "signature": "(path: 'Path') -> 'str'"
    },
    "parse_binding_segment": {
      "kind": "function",
      "signature": "(content: 'bytes') -> 'tuple[int, list[dict[str, object]]]'"
    },
    "parse_journal": {
      "kind": "function",
      "signature": "(content: 'bytes') -> 'tuple[JournalFrame, ...]'"
    },
    "parse_provenance_sequence": {
      "kind": "function",
      "signature": "(value: 'object', label: 'str' = 'provenance sequence') -> 'int'"
    },
    "prepare_file_provenance": {
      "kind": "function",
      "signature": "(payload: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver | None' = None, provenance: 'Path | None' = None, omit_reason: 'str | None' = None) -> 'PreparedFileProvenance'"
    },
    "provenance_journal_filename": {
      "kind": "function",
      "signature": "(journal_id: 'str') -> 'str'"
    },
    "resolve_incremental_journal_current_state": {
      "kind": "function",
      "signature": "(*, primary_lineage_id: 'str', binding_json: 'str', state_json: 'str') -> 'tuple[str, str, int, str]'"
    },
    "resolve_provenance_observer": {
      "kind": "function",
      "signature": "(name: 'str') -> 'ResolvedProvenanceObserver'"
    },
    "software_agent_id": {
      "kind": "function",
      "signature": "(name: 'str') -> 'str'"
    },
    "update_ordered_volume_commitment": {
      "kind": "function",
      "signature": "(digest: 'Any', document: 'ProvenanceSequenceDocument') -> 'None'"
    },
    "user_installation_id": {
      "kind": "function",
      "signature": "(application: 'str') -> 'str'"
    },
    "validate_entry_document": {
      "kind": "function",
      "signature": "(document: 'Mapping[str, Any]') -> 'None'"
    },
    "validate_graph_fragment": {
      "kind": "function",
      "signature": "(fragment: 'Mapping[str, Any]', *, contract_schemas: 'Mapping[str, Mapping[str, Any]] | None' = None, require_known_schemas: 'bool' = False) -> 'None'"
    },
    "validate_incremental_journal_entry": {
      "kind": "function",
      "signature": "(encoded: 'bytes', *, sequence: 'int', journal_id: 'str', previous_entry_id: 'str | None', previous_json_sha256: 'str | None') -> 'IncrementalJournalEntry'"
    },
    "validate_journal": {
      "kind": "function",
      "signature": "(content: 'bytes') -> 'JournalSummary'"
    },
    "validate_journal_chunks": {
      "kind": "function",
      "signature": "(chunks: 'Iterable[bytes]', *, retain_frames: 'bool' = True) -> 'JournalSummary'"
    },
    "validate_journal_set": {
      "kind": "function",
      "signature": "(journals: 'Mapping[str, bytes]') -> 'dict[str, JournalSummary]'"
    },
    "verify_payload_binding": {
      "kind": "function",
      "signature": "(summary: 'JournalSummary', *, path: 'str', byte_count: 'int', sha256: 'str') -> 'None'"
    }
  },
  "module": "riverhog_provenance"
}
```
