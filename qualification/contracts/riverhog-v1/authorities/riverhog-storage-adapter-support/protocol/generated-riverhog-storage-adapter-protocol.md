# generated:riverhog-storage-adapter protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-protocol:face8018ab -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `protocol` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/authorities`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/bundle_sha256`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/compatibility`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/format`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/protocol`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/semantic_acceptance`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

- Shape: array (7 items)

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/authorities`

<!-- exact-contract-value: 723bdc629c692bd71cc54f046e71fc791297a8c6a984b85d1f0962ee4c72194e -->

```json
{
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/bundle_sha256`

<!-- exact-contract-value: bba1c0f37d1dd1ece4258a32aaacd0bd885304ea010108064dcbe6452200c749 -->

```json
"ed6a7530471c36528ec0c944a389ae5b9f06bb3f643b45ff61c0f8bd47cc4ec6"
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/compatibility`

<!-- exact-contract-value: 5c7132cf2948fe916c565bddbbe06263bbac476702adb97867d0af3e9d24a284 -->

```json
{
  "provider_ontology": "private",
  "unknown_fields": "reject"
}
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/format`

<!-- exact-contract-value: 0ee9723360880ab353d34848d40bebcae178bfc6996bc4ff87d6a79fe6c54109 -->

```json
"riverhog-storage-adapter-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding`

<!-- exact-contract-value: 16fc8e3edc888a13c388c356ba98e5a419f9a4a96330ed816e1e5e9e76f10b84 -->

```json
{
  "operations": [
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        }
      ],
      "method": "GET",
      "path": "/v1/adapter",
      "path_parameters": [],
      "request": {
        "kind": "none",
        "schema": null
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "AdapterDescriptor",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "insufficient_storage",
          "status": 507
        },
        {
          "code": "invalid_path",
          "status": 400
        }
      ],
      "method": "POST",
      "path": "/v1/writes/begin",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "WriteStartRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "WriteSession",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "length_required",
          "status": 411
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        }
      ],
      "method": "POST",
      "path": "/v1/writes/segment",
      "path_parameters": [],
      "request": {
        "kind": "framed",
        "schema": "WriteSegmentRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "WriteSegmentReceipt",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "traversal_invalidated",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/writes/segments",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "WriteSegmentListRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "WriteSegmentPage",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "identity_conflict",
          "status": 409
        },
        {
          "code": "integrity_failure",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/writes/complete",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "WriteCompleteRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "CompletedObjectReceipt",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "identity_conflict",
          "status": 409
        },
        {
          "code": "integrity_failure",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/writes/completed",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "CompletedWriteLookupRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "CompletedObjectReceipt",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        }
      ],
      "method": "POST",
      "path": "/v1/writes/abort",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "WriteSession"
      },
      "response": {
        "headers": [],
        "kind": "none",
        "schema": null,
        "statuses": [
          204
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "length_required",
          "status": 411
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "insufficient_storage",
          "status": 507
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "identity_conflict",
          "status": 409
        },
        {
          "code": "integrity_failure",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/objects/put",
      "path_parameters": [],
      "request": {
        "kind": "framed",
        "schema": "SmallObjectWriteRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "ImmutableObjectReceipt",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "identity_conflict",
          "status": 409
        },
        {
          "code": "integrity_failure",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/objects/head",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "ObjectHeadRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "ObjectMetadataReceipt",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "invalid_range",
          "status": 416
        },
        {
          "code": "read_not_ready",
          "status": 409
        },
        {
          "code": "read_expired",
          "status": 409
        },
        {
          "code": "integrity_failure",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/objects/read",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "ObjectReadRequest"
      },
      "response": {
        "headers": [
          {
            "description": "Exact framed response-body length in bytes.",
            "name": "Content-Length",
            "schema": {
              "type": "string"
            }
          },
          {
            "description": "Adapter-observed complete object length in bytes.",
            "name": "X-Riverhog-Object-Bytes",
            "schema": {
              "type": "string"
            }
          },
          {
            "description": "Opaque provider revision when one exists.",
            "name": "X-Riverhog-Object-Revision",
            "schema": {
              "type": "string"
            }
          },
          {
            "description": "Exact returned range for a nonempty ranged read.",
            "name": "Content-Range",
            "schema": {
              "type": "string"
            }
          }
        ],
        "kind": "framed",
        "schema": "ObjectReadReceipt",
        "statuses": [
          200,
          206
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        }
      ],
      "method": "POST",
      "path": "/v1/objects/delete",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "DeleteObjectRequest"
      },
      "response": {
        "headers": [],
        "kind": "none",
        "schema": null,
        "statuses": [
          204
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        }
      ],
      "method": "POST",
      "path": "/v1/objects/delete-prefix",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "DeletePrefixRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "MaintenanceResult",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "read_not_ready",
          "status": 409
        },
        {
          "code": "read_expired",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/reads/prepare",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "ReadPreparationRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "ReadStatus",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "read_not_ready",
          "status": 409
        },
        {
          "code": "read_expired",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/reads/status",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "ReadPreparationRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "ReadStatus",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "StorageAdapterError",
      "errors": [
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "invalid_request",
          "status": 400
        },
        {
          "code": "provider_unavailable",
          "status": 503
        },
        {
          "code": "internal_failure",
          "status": 500
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "invalid_path",
          "status": 400
        },
        {
          "code": "not_found",
          "status": 404
        },
        {
          "code": "read_not_ready",
          "status": 409
        },
        {
          "code": "read_expired",
          "status": 409
        }
      ],
      "method": "POST",
      "path": "/v1/reads/cleanup",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "ReadPreparationRequest"
      },
      "response": {
        "headers": [],
        "kind": "none",
        "schema": null,
        "statuses": [
          204
        ]
      }
    }
  ]
}
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/protocol`

<!-- exact-contract-value: 979c8622598850c976d2a3a4e6f0b9675ba24e5d65f84606d91ff53c8dee4833 -->

```json
"riverhog-storage-adapter/v1"
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/semantic_acceptance`

<!-- exact-contract-value: ffedd3ad65cd966759c96e4bf07976fc6090a5f979142607868df5efed0c3cd8 -->

```json
{
  "conformance": "riverhog-storage-adapter-conformance-result/v1",
  "kind": "session-and-object-relations"
}
```
