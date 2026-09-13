# stove0_target_support.TargetExecutionFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionfailure:e97bcf4b8f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ab657fbd5"></a>
| Field | Shape |
|---|---|
| <a id="s-5ca613350d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-11b99ab7e8"></a>`distribution` | "stove0-target-support" |
| <a id="s-1a63801789"></a>`module` | "stove0_target_support" |
| <a id="s-ff897f6e1e"></a>`name` | "TargetExecutionFailure" |
| <a id="s-38d9e20e6b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-779c1a0e95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea5cb13d0f370b17a03ef50a163ba98963c53ab8e71abac0aa3448de4a8e8c51 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(code: 'str', message: 'str', *, retryable: 'bool') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetExecutionFailure",
  "unit": "export"
}
```
