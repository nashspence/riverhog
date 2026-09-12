# stove0_target_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client:63242e0d6d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-9d700f6e49c8) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2e5e0a4832ad"></a>
| Field | Shape |
|---|---|
| <a id="s-08ba1ea7618f"></a>`distribution` | "stove0-target-client" |
| <a id="s-908f6494d3a2"></a>`exports` | additional keys=`TargetCallbackClient`, `TargetClient`, `TargetProtocolError` |
| <a id="s-6e59ef568f1e"></a>`module` | "stove0_target_client" |

## Governing policies

- <a id="pa-fe221101625a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client](../../../evidence/sources.md#src-23a662423507) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py::<module>`

### Machine authority

- `/external_contract/python/22`

### Exact owned JSON

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
