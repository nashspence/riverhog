# generated:stove0-target protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-protocol:2545863062 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [protocol](index.md#f-1707db8d07b6) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Subject | Shape |
|---|---|
| <a id="s-459baccad426"></a>`authorities` | additional keys=`http_operations`, `semantic_acceptance`, `structural_models` |
| <a id="s-1181fd9feae6"></a>`bundle_sha256` | "916d26a1630afc12fe746ce28f8ba0f2639264e14929dd60b02c6c9059444b80" |
| <a id="s-9269284132d6"></a>`compatibility` | additional keys=`contract_identity`, `unknown_fields`, `unknown_protocol_revision` |
| <a id="s-e51e46fd849c"></a>`format` | "stove0-target-schema-bundle/v1" |
| <a id="s-eb3b11163120"></a>`http_binding` | additional keys=`operations` |
| <a id="s-aeb93f5dc602"></a>`protocols` | ["stove0-transform-target/v1","stove0-effect-target/v1"] |
| <a id="s-19ff2c96762f"></a>`semantic_acceptance` | additional keys=`binding`, `identity`, `kind`, `request_response_relations` |

## Governing policies

- <a id="pa-718adddb5004"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-target](../../../evidence/sources.md#src-2c42f9d39a0b) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/authorities`
- `/external_contract/protocol_schemas/generated:stove0-target/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-target/compatibility`
- `/external_contract/protocol_schemas/generated:stove0-target/format`
- `/external_contract/protocol_schemas/generated:stove0-target/http_binding`
- `/external_contract/protocol_schemas/generated:stove0-target/protocols`
- `/external_contract/protocol_schemas/generated:stove0-target/semantic_acceptance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:stove0-target/authorities`

<!-- exact-contract-value: 723bdc629c692bd71cc54f046e71fc791297a8c6a984b85d1f0962ee4c72194e -->

```json
{
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/bundle_sha256`

<!-- exact-contract-value: ffd3e8eb99f8691bc44129539a9b36eab6e0c3d559d54326a84916f6e55e374d -->

```json
"916d26a1630afc12fe746ce28f8ba0f2639264e14929dd60b02c6c9059444b80"
```

### `/external_contract/protocol_schemas/generated:stove0-target/compatibility`

<!-- exact-contract-value: fb2ae5072cf53b5181c923503273657248116b12ab23daed566f2fd061e53e27 -->

```json
{
  "contract_identity": "rfc8785-sha256",
  "unknown_fields": "reject",
  "unknown_protocol_revision": "reject"
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/format`

<!-- exact-contract-value: 6844917b9246b56594c9d97f148456b7e55cc6a0d13cb3cad09d1d8af02666c7 -->

```json
"stove0-target-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:stove0-target/http_binding`

<!-- exact-contract-value: 8ff0bb603ce15b51f622e26ba1eca5546d6e60ee37261b0909aba550d0a6691d -->

```json
{
  "operations": [
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "bad_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "target_failed",
          "status": 500
        }
      ],
      "method": "GET",
      "path": "/v1/target",
      "path_parameters": [],
      "request": {
        "kind": "none",
        "schema": null
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "TargetContract",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "invalid_target_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "target_protocol_mismatch",
          "status": 409
        },
        {
          "code": "operation_contract_mismatch",
          "status": 409
        },
        {
          "code": "unsupported_operation",
          "status": 400
        },
        {
          "code": "target_failed",
          "status": 500
        }
      ],
      "method": "POST",
      "path": "/v1/preflight",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "TargetPreflightRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "TargetPreflightResponse",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "invalid_target_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "job_identity_mismatch",
          "status": 409
        },
        {
          "code": "target_contract_mismatch",
          "status": 409
        },
        {
          "code": "operation_contract_mismatch",
          "status": 409
        },
        {
          "code": "job_request_mismatch",
          "status": 409
        },
        {
          "code": "target_runtime_mismatch",
          "status": 409
        },
        {
          "code": "unsupported_operation",
          "status": 400
        },
        {
          "code": "target_failed",
          "status": 500
        }
      ],
      "method": "PUT",
      "path": "/v1/jobs/{job_id}",
      "path_parameters": [
        {
          "name": "job_id",
          "schema": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          }
        }
      ],
      "request": {
        "kind": "json",
        "schema": "TargetJobRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "TargetJobStatus",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "bad_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "job_not_found",
          "status": 404
        },
        {
          "code": "target_failed",
          "status": 500
        }
      ],
      "method": "GET",
      "path": "/v1/jobs/{job_id}",
      "path_parameters": [
        {
          "name": "job_id",
          "schema": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          }
        }
      ],
      "request": {
        "kind": "none",
        "schema": null
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "TargetJobStatus",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "bad_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "job_not_found",
          "status": 404
        },
        {
          "code": "target_failed",
          "status": 500
        }
      ],
      "method": "POST",
      "path": "/v1/jobs/{job_id}/cancel",
      "path_parameters": [
        {
          "name": "job_id",
          "schema": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          }
        }
      ],
      "request": {
        "kind": "none",
        "schema": null
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "TargetJobStatus",
        "statuses": [
          200
        ]
      }
    }
  ]
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/protocols`

<!-- exact-contract-value: 6fa34737ad43c7376f68219f2deb607e97cb65bd84cd3d10c9372dd01ddb8d77 -->

```json
[
  "stove0-transform-target/v1",
  "stove0-effect-target/v1"
]
```

### `/external_contract/protocol_schemas/generated:stove0-target/semantic_acceptance`

<!-- exact-contract-value: 69c1750b5af0c70f85cd8837f26d3095cd1eff97666f606198ef17766c2167a8 -->

```json
{
  "binding": "OperationContract.intent_semantics",
  "identity": [
    "id",
    "profile_sha256"
  ],
  "kind": "operation-contract",
  "request_response_relations": "required"
}
```
