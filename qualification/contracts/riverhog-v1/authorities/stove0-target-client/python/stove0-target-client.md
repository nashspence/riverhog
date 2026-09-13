# stove0_target_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client:2c1ea34a9d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9ba74815b"></a>
| Field | Shape |
|---|---|
| <a id="s-6ad9f61d7e"></a>`candidate_id` | "python:stove0-target-client:stove0_target_client" |
| <a id="s-4dde7b1d75"></a>`distribution` | "stove0-target-client" |
| <a id="s-81fe53aa96"></a>`exports` | additional keys=`TargetCallbackClient`, `TargetClient`, `TargetProtocolError` |
| <a id="s-1971a3a435"></a>`module` | "stove0_target_client" |

## Governing policies

- <a id="pa-8380cf9e1e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/59`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03335ec07ccbc348c8b053d40335bb01f053c2aa50ceb079cc5fa3dadf86619a -->

```json
{
  "candidate_id": "python:stove0-target-client:stove0_target_client",
  "distribution": "stove0-target-client",
  "exports": {
    "TargetCallbackClient": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "declare_target_execution_disposition": {
          "kind": "method",
          "signature": "\"(self, job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'TargetCallbackAcknowledgement'\""
        },
        "declare_target_execution_output": {
          "kind": "method",
          "signature": "\"(self, job_id: 'str', output: 'OutputArtifact') -> 'TargetCallbackAcknowledgement'\""
        },
        "declare_target_execution_source_edge": {
          "kind": "method",
          "signature": "\"(self, job_id: 'str', edge: 'OutputSourceEdge') -> 'TargetCallbackAcknowledgement'\""
        },
        "get_target_execution_inputs": {
          "kind": "method",
          "signature": "\"(self, job_id: 'str', *, continuation: 'str | None' = None) -> 'TargetInputPage'\""
        },
        "iter_inputs": {
          "kind": "method",
          "signature": "\"(self, job_id: 'str') -> 'Iterator[InputArtifact]'\""
        },
        "seal_target_execution_production": {
          "kind": "method",
          "signature": "\"(self, job_id: 'str') -> 'TargetProductionSealResponse'\""
        }
      },
      "signature": "\"(access: 'TargetCallbackAccess', *, timeout: 'float | None' = 300.0) -> 'None'\""
    },
    "TargetClient": {
      "kind": "class",
      "members": {
        "cancel": {
          "kind": "method",
          "signature": "\"(self, request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        },
        "contract": {
          "kind": "method",
          "signature": "\"(self) -> 'TargetContract'\""
        },
        "preflight": {
          "kind": "method",
          "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
        },
        "put_job": {
          "kind": "method",
          "signature": "\"(self, request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        },
        "status": {
          "kind": "method",
          "signature": "\"(self, request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        }
      },
      "signature": "\"(base_url: 'str', *, token: 'str | None' = None, timeout: 'float | None' = 300.0, allow_insecure_http: 'bool' = False) -> 'None'\""
    },
    "TargetProtocolError": {
      "kind": "class",
      "signature": "'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\']\", code: \\'str | None\\' = None, observed_status: \\'int | None\\' = None, details: \\'Mapping[str, Any] | None\\' = None) -> \\'None\\''"
    }
  },
  "module": "stove0_target_client"
}
```
