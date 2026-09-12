# riverhog_storage_adapter_protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol:4be9f0aa0d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-protocol` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `distribution` | "riverhog-storage-adapter-protocol" |
| `exports` | additional keys=`ADAPTER_PRIVATE_ASSERTION_PREFIX`, `AdapterDescriptor`, `BinaryContent`, `CompletedObjectReceipt`, `CompletedWriteLookupRequest`, `DeleteObjectRequest`, `DeletePrefixRequest`, `ImmutableObjectReceipt`, `MAX_WRITE_SEGMENT_PAGE_ITEMS`, `MaintenanceResult`, `ObjectHeadRequest`, `ObjectLocator`, `ObjectMetadataReceipt`, `ObjectPlacement`, `ObjectReadReceipt`, `ObjectReadRequest`, `ObjectReadStream`, `ReadExpired`, `ReadMode`, `ReadPreparationRequest`, `ReadReadiness`, `ReadReady`, `ReadRequested`, `ReadStatus`, `RequiredIdentityAssertions`, `STORAGE_ADAPTER_PROTOCOL`, `SemanticId`, `Sha256`, `SmallObjectWriteRequest`, `StorageAdapterError`, `StorageAdapterErrorBody`, `StorageAdapterErrorCode`, `StorageAdapterModel`, `StorageAdapterPort`, `StorageAdapterRejection`, `ValidatedStorageAdapterPort`, `WriteCompleteRequest`, `WriteCompletionAuthority`, `WriteSegmentListRequest`, `WriteSegmentPage`, `WriteSegmentReceipt`, `WriteSegmentRequest`, `WriteSession`, `WriteStartRequest`, `normalize_object_path`, `validate_completed_write_response`, `validate_object_metadata_response`, `validate_object_read_response`, `validate_read_status_response`, `validate_small_object_response`, `validate_write_completion_request`, `validate_write_segment_page_response`, `validate_write_segment_request`, `validate_write_segment_response`, `validate_write_session_response`, `validate_write_start_request`, `validated_storage_adapter` |
| `module` | "riverhog_storage_adapter_protocol" |

## Governing policies

- `compatibility/python-api/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:riverhog-storage-adapter-protocol` — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py::<module>`

### Machine authority

- `/external_contract/python/11`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4609a016a7fd52d9b596f8b7facecd4210dc4c934864cb8ed8435fd215d6e953 -->

```json
{
  "distribution": "riverhog-storage-adapter-protocol",
  "exports": {
    "ADAPTER_PRIVATE_ASSERTION_PREFIX": {
      "kind": "constant",
      "value": "riverhog-adapter-"
    },
    "AdapterDescriptor": {
      "kind": "class",
      "members": {
        "validate_segment_limits": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "9cee17550dd897297e60b7829813a6b432932a360b38b800c81292b1cbdda754",
      "signature": "(*, protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], read_mode: Literal['immediate', 'restore_required'], minimum_nonfinal_segment_bytes: Annotated[int, Ge(ge=1)], maximum_segment_bytes: Annotated[int | None, Ge(ge=1)] = None, maximum_segment_count: Annotated[int | None, Ge(ge=1)] = None) -> None"
    },
    "BinaryContent": {
      "kind": "object",
      "type": "types.UnionType"
    },
    "CompletedObjectReceipt": {
      "kind": "class",
      "members": {
        "canonical_completed_at": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        },
        "canonical_metadata": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, str]') -> 'dict[str, str]'"
        },
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "209031565e0ef889aecef270878eea3fc57d0b15a42c37d7610d1dbf208def6a",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=1)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None"
    },
    "CompletedWriteLookupRequest": {
      "kind": "class",
      "members": {
        "canonical_metadata": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, str]') -> 'dict[str, str]'"
        },
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "3f6289ea7cdb8562a130c3d3df28e4217ae0490a38d84f2f2158dafb98e18689",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None"
    },
    "DeleteObjectRequest": {
      "kind": "class",
      "members": {
        "validate_revision": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "46f15404c356f50e3a381b04f209d8f64b9114cce82ef9e8c42967f06b59fad8",
      "signature": "(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, mode: Literal['current', 'exact_revision', 'all_versions'], expected_current_stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None"
    },
    "DeletePrefixRequest": {
      "kind": "class",
      "members": {
        "canonical_prefix": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "26f1798a7e22e1a72e0c6932bfbe66dca3e80273a417c3e5c18fdc0e4bd59a3f",
      "signature": "(*, object_prefix: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], mode: Literal['all_versions'] = 'all_versions') -> None"
    },
    "ImmutableObjectReceipt": {
      "kind": "class",
      "members": {
        "canonical_completed_at": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        },
        "canonical_metadata": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, str]') -> 'dict[str, str]'"
        },
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "4246d342ca4bea1f0e76e6b31ee8d998513e8d604a1b2ef888c96c7a0776196d",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None"
    },
    "MAX_WRITE_SEGMENT_PAGE_ITEMS": {
      "kind": "constant",
      "value": 128
    },
    "MaintenanceResult": {
      "kind": "class",
      "schema_sha256": "1b8ad376bb41a2eb81c64450d4ccb99a09150f8f1c72f6203ecdad2b933a565c",
      "signature": "(*, affected: Annotated[int, Ge(ge=0)]) -> None"
    },
    "ObjectHeadRequest": {
      "kind": "class",
      "schema_sha256": "1e0283dd234d9ca0e0f4a026cfc1d8102dd40e8a31f86eb9e99e8613976200e5",
      "signature": "(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_placement: Literal['archive', 'immediate']) -> None"
    },
    "ObjectLocator": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "5728d2076704f8b4113eabaab445c0f91b667082021eab79d06d5f97438ce1cb",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None) -> None"
    },
    "ObjectMetadataReceipt": {
      "kind": "class",
      "members": {
        "canonical_completed_at": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        },
        "canonical_metadata": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, str]') -> 'dict[str, str]'"
        },
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "10fe081f0530594bffeb9d17545b2652dd8db57725461bb29fe06922e61a7c67",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, content_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observed_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None"
    },
    "ObjectPlacement": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "ObjectReadReceipt": {
      "kind": "class",
      "members": {
        "validate_range": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "c00ab742b44d1b708bac1d252dd1dd6fb4882bdec441eb330f63746d635820ce",
      "signature": "(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, total_bytes: Annotated[int, Ge(ge=0)], offset: Annotated[int, Ge(ge=0)], read_bytes: Annotated[int, Ge(ge=0)]) -> None"
    },
    "ObjectReadRequest": {
      "kind": "class",
      "members": {
        "validate_range": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "8f080cf57778767bde220ff2111c9ba3501ad753c8492b1759db04b8451d108d",
      "signature": "(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_bytes: Annotated[int, Ge(ge=0)], offset: Annotated[int | None, Ge(ge=0)] = None, size: Annotated[int | None, Ge(ge=0)] = None) -> None"
    },
    "ObjectReadStream": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        }
      },
      "signature": "(*, receipt: 'ObjectReadReceipt', content: 'Iterator[bytes]', close: 'Callable[[], None] | None' = None) -> 'None'"
    },
    "ReadExpired": {
      "kind": "class",
      "schema_sha256": "3a7d5d1ad6056eb379318b3aba88c1d36fb5a6cfea9d6b2bb1675751cf4bd2c2",
      "signature": "(*, state: Literal['expired'] = 'expired') -> None"
    },
    "ReadMode": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "ReadPreparationRequest": {
      "kind": "class",
      "members": {
        "canonical_objects": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ObjectLocator, ...]') -> 'tuple[ObjectLocator, ...]'"
        }
      },
      "schema_sha256": "a1096dde3495131448c778944da805d24cbfabda7630aa9d927684235cb42635",
      "signature": "(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)]) -> None"
    },
    "ReadReadiness": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "ReadReady": {
      "kind": "class",
      "members": {
        "canonical_available_until": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str | None') -> 'str | None'"
        }
      },
      "schema_sha256": "e82ea0df8857c679ccf90afa1702480b6aca83c3226500f6805d2ae25099ba53",
      "signature": "(*, state: Literal['ready'] = 'ready', available_until: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None"
    },
    "ReadRequested": {
      "kind": "class",
      "members": {
        "canonical_estimated_ready_at": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str | None') -> 'str | None'"
        }
      },
      "schema_sha256": "d31c1bb220eaafa6bbc0ed7ed7faaba5eaa8d145766373dbbc76f7f5f377c2e7",
      "signature": "(*, state: Literal['requested'] = 'requested', estimated_ready_at: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None"
    },
    "ReadStatus": {
      "kind": "class",
      "members": {
        "canonical_objects": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ObjectLocator, ...]') -> 'tuple[ObjectLocator, ...]'"
        }
      },
      "schema_sha256": "c2e15c25cf8ffc82c41b134eb06692ae3c3138226cfdc381c64e277774f403a6",
      "signature": "(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)], readiness: riverhog_storage_adapter_protocol.protocol.ReadRequested | riverhog_storage_adapter_protocol.protocol.ReadReady | riverhog_storage_adapter_protocol.protocol.ReadExpired) -> None"
    },
    "RequiredIdentityAssertions": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "STORAGE_ADAPTER_PROTOCOL": {
      "kind": "constant",
      "value": "riverhog-storage-adapter/v1"
    },
    "SemanticId": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "Sha256": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "SmallObjectWriteRequest": {
      "kind": "class",
      "members": {
        "canonical_metadata": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, str]') -> 'dict[str, str]'"
        },
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        },
        "validate_replacement_fence": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "80a724477b2bedfe5fa69bfd96c571cd25a684c741a0e8d5a715c15bfc25c270",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate'], mode: Literal['create_only', 'replace_current'], expected_current_stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "StorageAdapterError": {
      "kind": "class",
      "schema_sha256": "19b906afcdf8d94d77f5aef65b4facf0cb084d382ef94d440ae8a5cd58e7aa7f",
      "signature": "(*, error: riverhog_storage_adapter_protocol.protocol.StorageAdapterErrorBody) -> None"
    },
    "StorageAdapterErrorBody": {
      "kind": "class",
      "schema_sha256": "9c6d2bb482f12292a4cb9eca4a79d1e6c953362f34ba69a7f5009bf0d394616d",
      "signature": "(*, code: Literal['unauthorized', 'invalid_request', 'not_found', 'method_not_allowed', 'length_required', 'request_too_large', 'insufficient_storage', 'identity_conflict', 'traversal_invalidated', 'invalid_path', 'invalid_range', 'read_not_ready', 'read_expired', 'integrity_failure', 'provider_unavailable', 'internal_failure'], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2000)]) -> None"
    },
    "StorageAdapterErrorCode": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "StorageAdapterModel": {
      "kind": "class",
      "schema_sha256": "571e8620bee52a4520a602c0fda40f9631fb2fa449d9b858070ec0a5491c8add",
      "signature": "() -> None"
    },
    "StorageAdapterPort": {
      "kind": "class",
      "members": {
        "abort_write": {
          "kind": "method",
          "signature": "(self, session: 'WriteSession') -> 'None'"
        },
        "begin_write": {
          "kind": "method",
          "signature": "(self, request: 'WriteStartRequest') -> 'WriteSession'"
        },
        "cleanup_read": {
          "kind": "method",
          "signature": "(self, request: 'ReadPreparationRequest') -> 'None'"
        },
        "complete_write": {
          "kind": "method",
          "signature": "(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'"
        },
        "delete_object": {
          "kind": "method",
          "signature": "(self, request: 'DeleteObjectRequest') -> 'None'"
        },
        "delete_prefix": {
          "kind": "method",
          "signature": "(self, request: 'DeletePrefixRequest') -> 'int'"
        },
        "descriptor": {
          "kind": "method",
          "signature": "(self) -> 'AdapterDescriptor'"
        },
        "find_completed_write": {
          "kind": "method",
          "signature": "(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt | None'"
        },
        "head_object": {
          "kind": "method",
          "signature": "(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt | None'"
        },
        "list_segments": {
          "kind": "method",
          "signature": "(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'"
        },
        "prepare_read": {
          "kind": "method",
          "signature": "(self, request: 'ReadPreparationRequest') -> 'ReadStatus'"
        },
        "put_small_object": {
          "kind": "method",
          "signature": "(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'"
        },
        "read_object": {
          "kind": "method",
          "signature": "(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'"
        },
        "read_status": {
          "kind": "method",
          "signature": "(self, request: 'ReadPreparationRequest') -> 'ReadStatus'"
        },
        "write_segment": {
          "kind": "method",
          "signature": "(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "StorageAdapterRejection": {
      "kind": "class",
      "signature": "(code: 'StorageAdapterErrorCode', message: 'str') -> 'None'"
    },
    "ValidatedStorageAdapterPort": {
      "kind": "class",
      "members": {
        "abort_write": {
          "kind": "method",
          "signature": "(self, session: 'WriteSession') -> 'None'"
        },
        "begin_write": {
          "kind": "method",
          "signature": "(self, request: 'WriteStartRequest') -> 'WriteSession'"
        },
        "cleanup_read": {
          "kind": "method",
          "signature": "(self, request: 'ReadPreparationRequest') -> 'None'"
        },
        "complete_write": {
          "kind": "method",
          "signature": "(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'"
        },
        "delete_object": {
          "kind": "method",
          "signature": "(self, request: 'DeleteObjectRequest') -> 'None'"
        },
        "delete_prefix": {
          "kind": "method",
          "signature": "(self, request: 'DeletePrefixRequest') -> 'int'"
        },
        "descriptor": {
          "kind": "method",
          "signature": "(self) -> 'AdapterDescriptor'"
        },
        "find_completed_write": {
          "kind": "method",
          "signature": "(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt | None'"
        },
        "head_object": {
          "kind": "method",
          "signature": "(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt | None'"
        },
        "list_segments": {
          "kind": "method",
          "signature": "(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'"
        },
        "prepare_read": {
          "kind": "method",
          "signature": "(self, request: 'ReadPreparationRequest') -> 'ReadStatus'"
        },
        "put_small_object": {
          "kind": "method",
          "signature": "(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'"
        },
        "read_object": {
          "kind": "method",
          "signature": "(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'"
        },
        "read_status": {
          "kind": "method",
          "signature": "(self, request: 'ReadPreparationRequest') -> 'ReadStatus'"
        },
        "write_segment": {
          "kind": "method",
          "signature": "(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'"
        }
      },
      "signature": "(adapter: 'StorageAdapterPort') -> 'None'"
    },
    "WriteCompleteRequest": {
      "kind": "class",
      "members": {
        "canonical_metadata": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, str]') -> 'dict[str, str]'"
        },
        "validate_bytes": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "3b6857dbea430d76de035e4b9a4d2e0306c6b78a8f4fd827d729da581b4e9f10",
      "signature": "(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority, expected_bytes: Annotated[int, Ge(ge=1)], expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None"
    },
    "WriteCompletionAuthority": {
      "kind": "class",
      "schema_sha256": "c522a1b0d2d807b0740cab37856acf28b1c1eea4fa6a576110441995d96589d7",
      "signature": "(*, segment_count: Annotated[int, Ge(ge=0)], stored_bytes: Annotated[int, Ge(ge=0)], authority_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None"
    },
    "WriteSegmentListRequest": {
      "kind": "class",
      "schema_sha256": "c655ec3f5640ade7ac2d3b60a3b9bb3de1ecaf39de3d4bcd731afb029a65684f",
      "signature": "(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, after_number: Annotated[int, Ge(ge=0)] = 0, traversal_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, maximum_items: Annotated[int, Ge(ge=1), Le(le=128)] = 128) -> None"
    },
    "WriteSegmentPage": {
      "kind": "class",
      "members": {
        "canonical_segments": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[WriteSegmentReceipt, ...]') -> 'tuple[WriteSegmentReceipt, ...]'"
        },
        "validate_terminal": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "d1d971aecf4fda3444d770576aa32bb6527302511e9401a9b027918ec8ae3b38",
      "signature": "(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, traversal_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)], segments: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.WriteSegmentReceipt, ...], MaxLen(max_length=128)] = (), next_after_number: Annotated[int | None, Ge(ge=1)] = None, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority | None = None) -> None"
    },
    "WriteSegmentReceipt": {
      "kind": "class",
      "schema_sha256": "78f82d84ff68316a37a5d4d95d212d082ea0eada74506b6745aee67978f169da",
      "signature": "(*, number: Annotated[int, Ge(ge=1)], segment_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)], stored_bytes: Annotated[int, Ge(ge=1)], stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None"
    },
    "WriteSegmentRequest": {
      "kind": "class",
      "schema_sha256": "e51e90e06c0f6a6ca83920b56cbb0696ced3950650ea8d743c33191cb2cf7d31",
      "signature": "(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, number: Annotated[int, Ge(ge=1)], stored_bytes: Annotated[int, Ge(ge=1)]) -> None"
    },
    "WriteSession": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "8273b4a0c12b6fa55f8669267e1ed4d9a0091e43944c85d6f7e1c16aa6e892ac",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], write_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None"
    },
    "WriteStartRequest": {
      "kind": "class",
      "members": {
        "canonical_metadata": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, str]') -> 'dict[str, str]'"
        },
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "4394746d6639230cabebf5d57a6e369f5e1401fe144a77641c7eb0aff144d2fa",
      "signature": "(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate']) -> None"
    },
    "normalize_object_path": {
      "kind": "function",
      "signature": "(value: 'str', *, allow_prefix: 'bool' = False) -> 'str'"
    },
    "validate_completed_write_response": {
      "kind": "function",
      "signature": "(request: 'WriteCompleteRequest | CompletedWriteLookupRequest', response: 'CompletedObjectReceipt') -> 'None'"
    },
    "validate_object_metadata_response": {
      "kind": "function",
      "signature": "(request: 'ObjectHeadRequest', response: 'ObjectMetadataReceipt') -> 'None'"
    },
    "validate_object_read_response": {
      "kind": "function",
      "signature": "(request: 'ObjectReadRequest', response: 'ObjectReadReceipt') -> 'None'"
    },
    "validate_read_status_response": {
      "kind": "function",
      "signature": "(request: 'ReadPreparationRequest', response: 'ReadStatus') -> 'None'"
    },
    "validate_small_object_response": {
      "kind": "function",
      "signature": "(request: 'SmallObjectWriteRequest', response: 'ImmutableObjectReceipt') -> 'None'"
    },
    "validate_write_completion_request": {
      "kind": "function",
      "signature": "(request: 'WriteCompleteRequest', descriptor: 'AdapterDescriptor') -> 'None'"
    },
    "validate_write_segment_page_response": {
      "kind": "function",
      "signature": "(request: 'WriteSegmentListRequest', response: 'WriteSegmentPage', descriptor: 'AdapterDescriptor') -> 'None'"
    },
    "validate_write_segment_request": {
      "kind": "function",
      "signature": "(request: 'WriteSegmentRequest', descriptor: 'AdapterDescriptor') -> 'None'"
    },
    "validate_write_segment_response": {
      "kind": "function",
      "signature": "(request: 'WriteSegmentRequest', response: 'WriteSegmentReceipt') -> 'None'"
    },
    "validate_write_session_response": {
      "kind": "function",
      "signature": "(request: 'WriteStartRequest', response: 'WriteSession') -> 'None'"
    },
    "validate_write_start_request": {
      "kind": "function",
      "signature": "(request: 'WriteStartRequest', descriptor: 'AdapterDescriptor') -> 'None'"
    },
    "validated_storage_adapter": {
      "kind": "function",
      "signature": "(adapter: 'StorageAdapterPort') -> 'ValidatedStorageAdapterPort'"
    }
  },
  "module": "riverhog_storage_adapter_protocol"
}
```
