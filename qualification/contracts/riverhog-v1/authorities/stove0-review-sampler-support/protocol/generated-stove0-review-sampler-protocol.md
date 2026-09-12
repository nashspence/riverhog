# generated:stove0-review-sampler protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-protocol:8f6ec4c996 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `protocol` |
| Family | `protocol` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/authorities`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/format`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/http_binding`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/protocol`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/semantic_acceptance`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-review-sampler` — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`
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
  "dbbc9320e223fb591a30981339ba8c7a7f8b7de2358ce3a3cc38c2c7fbf78119",
  "stove0-review-sampler-schema-bundle/v1",
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
            "code": "sampler_failed",
            "status": 500
          }
        ],
        "method": "GET",
        "path": "/v1/sampler",
        "path_parameters": [],
        "request": {
          "kind": "none",
          "schema": null
        },
        "response": {
          "headers": [],
          "kind": "json",
          "schema": "SamplerDescriptor",
          "statuses": [
            200
          ]
        }
      },
      {
        "error_schema": "ErrorResponse",
        "errors": [
          {
            "code": "invalid_sampler_request",
            "status": 400
          },
          {
            "code": "unauthorized",
            "status": 401
          },
          {
            "code": "sampler_changed",
            "status": 409
          },
          {
            "code": "request_too_large",
            "status": 413
          },
          {
            "code": "sampler_failed",
            "status": 500
          }
        ],
        "method": "POST",
        "path": "/v1/sample",
        "path_parameters": [],
        "request": {
          "kind": "json",
          "schema": "SamplerRequest"
        },
        "response": {
          "headers": [],
          "kind": "json",
          "schema": "SamplerResult",
          "statuses": [
            200
          ]
        }
      }
    ]
  },
  "stove0-review-sampler/v1",
  {
    "kind": "request-bound-result",
    "validator": "validate_result"
  }
]
```
