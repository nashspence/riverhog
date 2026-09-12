# stove0_target_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client:63242e0d6d -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-client` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/22`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:stove0-target-client` — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `distribution` | "stove0-target-client" |
| `exports` | object (3 fields) |
| `module` | "stove0_target_client" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b3058a4540ed54acfdebc584b785d5b5042fcec5f815c4b188b76aa1629e387 -->

```json
{
  "distribution": "stove0-target-client",
  "exports": {
    "TargetCallbackClient": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "declare_target_execution_disposition": {
          "kind": "method",
          "signature": "(self, job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'TargetCallbackAcknowledgement'"
        },
        "declare_target_execution_output": {
          "kind": "method",
          "signature": "(self, job_id: 'str', output: 'OutputArtifact') -> 'TargetCallbackAcknowledgement'"
        },
        "declare_target_execution_source_edge": {
          "kind": "method",
          "signature": "(self, job_id: 'str', edge: 'OutputSourceEdge') -> 'TargetCallbackAcknowledgement'"
        },
        "get_target_execution_inputs": {
          "kind": "method",
          "signature": "(self, job_id: 'str', *, continuation: 'str | None' = None) -> 'TargetInputPage'"
        },
        "iter_inputs": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'Iterator[InputArtifact]'"
        },
        "seal_target_execution_production": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'TargetProductionSealResponse'"
        }
      },
      "signature": "(access: 'TargetCallbackAccess', *, timeout: 'float | None' = 300.0) -> 'None'"
    },
    "TargetClient": {
      "kind": "class",
      "members": {
        "cancel": {
          "kind": "method",
          "signature": "(self, request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'"
        },
        "contract": {
          "kind": "method",
          "signature": "(self) -> 'TargetContract'"
        },
        "preflight": {
          "kind": "method",
          "signature": "(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'"
        },
        "put_job": {
          "kind": "method",
          "signature": "(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'"
        },
        "status": {
          "kind": "method",
          "signature": "(self, request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'"
        }
      },
      "signature": "(base_url: 'str', *, token: 'str | None' = None, timeout: 'float | None' = 300.0, allow_insecure_http: 'bool' = False) -> 'None'"
    },
    "TargetProtocolError": {
      "kind": "class",
      "signature": "(message: 'str', *, failure_kind: \"Literal['remote_rejection', 'transport', 'invalid_response']\", code: 'str | None' = None, observed_status: 'int | None' = None, details: 'Mapping[str, Any] | None' = None) -> 'None'"
    }
  },
  "module": "stove0_target_client"
}
```
