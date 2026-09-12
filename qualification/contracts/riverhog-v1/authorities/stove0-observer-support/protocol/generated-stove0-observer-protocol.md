# generated:stove0-observer protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-protocol:f27802529a -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `protocol` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/authorities`
- `/external_contract/protocol_schemas/generated:stove0-observer/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-observer/compatibility`
- `/external_contract/protocol_schemas/generated:stove0-observer/format`
- `/external_contract/protocol_schemas/generated:stove0-observer/http_binding`
- `/external_contract/protocol_schemas/generated:stove0-observer/protocol`
- `/external_contract/protocol_schemas/generated:stove0-observer/semantic_acceptance`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-observer` — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`
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
  "d7ee01cb66cac26b457031d54882a95ce2b1ae0e163df53590fee00ae1a8f0ce",
  {
    "contract_identity": "canonical-json-sha256",
    "unknown_fields": "reject",
    "unknown_protocol_revision": "reject"
  },
  "stove0-observer-schema-bundle/v1",
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
            "code": "observer_failed",
            "status": 500
          }
        ],
        "method": "GET",
        "path": "/v1/observer",
        "path_parameters": [],
        "request": {
          "kind": "none",
          "schema": null
        },
        "response": {
          "headers": [],
          "kind": "json",
          "schema": "ObserverDescriptor",
          "statuses": [
            200
          ]
        }
      },
      {
        "error_schema": "ErrorResponse",
        "errors": [
          {
            "code": "invalid_observation_request",
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
            "code": "observer_failed",
            "status": 500
          }
        ],
        "method": "POST",
        "path": "/v1/observe",
        "path_parameters": [],
        "request": {
          "kind": "json",
          "schema": "ObservationInvocation"
        },
        "response": {
          "headers": [],
          "kind": "json",
          "schema": "ObservationResult",
          "statuses": [
            200
          ]
        }
      }
    ]
  },
  "stove0-content-observer/v1",
  {
    "binding": "ObserverContract.facts_semantics",
    "identity": [
      "id",
      "profile_sha256"
    ],
    "kind": "profile-registry",
    "unavailable_profile": "reject"
  }
]
```
