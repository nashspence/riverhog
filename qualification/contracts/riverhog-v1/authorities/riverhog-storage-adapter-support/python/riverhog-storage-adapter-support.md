# riverhog_storage_adapter_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support:2ea2497644 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-8798be8e4c3f) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7b1d96f90df2"></a>
| Field | Shape |
|---|---|
| <a id="s-816d959f749a"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-7b0c60bf1399"></a>`exports` | additional keys=`DEFAULT_MAXIMUM_HEADER_BYTES`, `FRAMED_BODY_FORMAT`, `FRAMED_BODY_MEDIA_TYPE`, `FRAMED_STORAGE_ADAPTER_HTTP_PATHS`, `FramedBodyError`, `FramedContent`, `STORAGE_ADAPTER_CONFORMANCE_RESULT`, `STORAGE_ADAPTER_HTTP_OPERATIONS`, `STORAGE_ADAPTER_SCHEMA_BUNDLE_FORMAT`, `StorageAdapterClient`, `StorageAdapterConformanceResult`, `StorageAdapterHttpBinding`, `StorageAdapterHttpResponse`, `StorageAdapterProtocolError`, `StorageAdapterServiceError`, `framed_body`, `framed_body_length`, `framed_declaration_bytes`, `parse_framed_stream`, `run_storage_adapter_conformance`, `storage_adapter_schema_bundle` |
| <a id="s-c8c4afa75418"></a>`module` | "riverhog_storage_adapter_support" |

## Governing policies

- <a id="pa-f2201148c0a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support](../../../evidence/sources.md#src-ce06974b7fef) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py::<module>`

### Machine authority

- `/external_contract/python/12`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6991392e4b552912e26282c57c02a748b5b4331d5af8d10cdaf368802b2639f9 -->

```json
{
  "distribution": "riverhog-storage-adapter-support",
  "exports": {
    "DEFAULT_MAXIMUM_HEADER_BYTES": {
      "kind": "constant",
      "value": 32768
    },
    "FRAMED_BODY_FORMAT": {
      "kind": "constant",
      "value": "riverhog-json-opaque-framing/v1"
    },
    "FRAMED_BODY_MEDIA_TYPE": {
      "kind": "constant",
      "value": "application/vnd.riverhog.json-opaque-framing"
    },
    "FRAMED_STORAGE_ADAPTER_HTTP_PATHS": {
      "kind": "constant",
      "value": [
        "/v1/objects/put",
        "/v1/writes/segment"
      ]
    },
    "FramedBodyError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "FramedContent": {
      "kind": "class",
      "members": {
        "require_consumed": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        }
      },
      "signature": "(chunks: 'Iterator[bytes]', expected_bytes: 'int') -> 'None'"
    },
    "STORAGE_ADAPTER_CONFORMANCE_RESULT": {
      "kind": "constant",
      "value": "riverhog-storage-adapter-conformance-result/v1"
    },
    "STORAGE_ADAPTER_HTTP_OPERATIONS": {
      "kind": "object",
      "type": "builtins.tuple"
    },
    "STORAGE_ADAPTER_SCHEMA_BUNDLE_FORMAT": {
      "kind": "constant",
      "value": "riverhog-storage-adapter-schema-bundle/v1"
    },
    "StorageAdapterClient": {
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
        "check_readiness": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "cleanup_read": {
          "kind": "method",
          "signature": "(self, request: 'ReadPreparationRequest') -> 'None'"
        },
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
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
        "from_token_file": {
          "kind": "classmethod",
          "signature": "(cls, base_url: 'str', *, token_file: 'Path', allow_insecure_http: 'bool' = False, timeout: 'float | httpx.Timeout | None' = 300.0, maximum_connections: 'int' = 32, client: 'httpx.Client | None' = None) -> 'StorageAdapterClient'"
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
      "signature": "(base_url: 'str', *, token: 'str', allow_insecure_http: 'bool' = False, timeout: 'float | httpx.Timeout | None' = 300.0, maximum_connections: 'int' = 32, client: 'httpx.Client | None' = None) -> 'None'"
    },
    "StorageAdapterConformanceResult": {
      "kind": "class",
      "members": {
        "validate_exact_coverage": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "af805afd4d0dffb329ba08dde971222ad008eab2aec7afc4abc0303dd1956caf",
      "signature": "(*, format: Literal['riverhog-storage-adapter-conformance-result/v1'] = 'riverhog-storage-adapter-conformance-result/v1', protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', status: Literal['conformant'] = 'conformant', coverage: Literal['complete'] = 'complete', descriptor: riverhog_storage_adapter_protocol.protocol.AdapterDescriptor, checks: tuple[str, ...]) -> None"
    },
    "StorageAdapterHttpBinding": {
      "kind": "class",
      "members": {
        "handle": {
          "kind": "method",
          "signature": "(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'StorageAdapterHttpResponse'"
        },
        "handle_framed": {
          "kind": "method",
          "signature": "(self, method: 'str', path: 'str', chunks: 'Iterable[bytes]', *, content_length: 'int | None') -> 'StorageAdapterHttpResponse'"
        }
      },
      "signature": "(adapter: 'StorageAdapterPort', *, maximum_control_bytes: 'int' = 67108864) -> 'None'"
    },
    "StorageAdapterHttpResponse": {
      "fields": [
        {
          "default": "required",
          "name": "status",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "headers",
          "type": "'tuple[tuple[str, str], ...]'"
        },
        {
          "default": "required",
          "name": "body",
          "type": "'bytes | Iterator[bytes]'"
        }
      ],
      "kind": "class",
      "signature": "(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes | Iterator[bytes]') -> None"
    },
    "StorageAdapterProtocolError": {
      "kind": "class",
      "signature": "(message: 'str', *, status_code: 'int | None' = None, code: 'StorageAdapterErrorCode' = 'internal_failure') -> 'None'"
    },
    "StorageAdapterServiceError": {
      "kind": "class",
      "signature": "(status: 'int', code: 'StorageAdapterErrorCode', message: 'str') -> 'None'"
    },
    "framed_body": {
      "kind": "function",
      "signature": "(model: 'BaseModel', content: 'BinaryContent') -> 'Iterator[bytes]'"
    },
    "framed_body_length": {
      "kind": "function",
      "signature": "(model: 'BaseModel') -> 'int'"
    },
    "framed_declaration_bytes": {
      "kind": "function",
      "signature": "(model: 'BaseModel') -> 'bytes'"
    },
    "parse_framed_stream": {
      "kind": "function",
      "signature": "(chunks: 'Iterable[bytes]', model: 'type[ModelT]', *, content_length: 'int', maximum_header_bytes: 'int' = 32768) -> 'tuple[ModelT, FramedContent]'"
    },
    "run_storage_adapter_conformance": {
      "kind": "function",
      "signature": "(client: 'StorageAdapterClient', *, continuation_client: 'StorageAdapterClient', object_prefix: 'str') -> 'StorageAdapterConformanceResult'"
    },
    "storage_adapter_schema_bundle": {
      "kind": "function",
      "signature": "() -> 'dict[str, Any]'"
    }
  },
  "module": "riverhog_storage_adapter_support"
}
```
