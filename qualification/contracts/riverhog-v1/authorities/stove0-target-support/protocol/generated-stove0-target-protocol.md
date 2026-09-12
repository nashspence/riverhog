# generated:stove0-target protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-protocol:2545863062 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `protocol` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/authorities`
- `/external_contract/protocol_schemas/generated:stove0-target/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-target/compatibility`
- `/external_contract/protocol_schemas/generated:stove0-target/format`
- `/external_contract/protocol_schemas/generated:stove0-target/http_binding`
- `/external_contract/protocol_schemas/generated:stove0-target/protocols`
- `/external_contract/protocol_schemas/generated:stove0-target/semantic_acceptance`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-target` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

```json
[
  {
    "http_operations": "http_binding.operations",
    "semantic_acceptance": "semantic_acceptance",
    "structural_models": "schemas"
  },
  "916d26a1630afc12fe746ce28f8ba0f2639264e14929dd60b02c6c9059444b80",
  {
    "contract_identity": "rfc8785-sha256",
    "unknown_fields": "reject",
    "unknown_protocol_revision": "reject"
  },
  "stove0-target-schema-bundle/v1",
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
  },
  [
    "stove0-transform-target/v1",
    "stove0-effect-target/v1"
  ],
  {
    "binding": "OperationContract.intent_semantics",
    "identity": [
      "id",
      "profile_sha256"
    ],
    "kind": "operation-contract",
    "request_response_relations": "required"
  }
]
```
