# stove0_target_support.TargetExecutionRuntime.from_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-f5c3c02480:665c47cc08 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e701a450b6"></a>
- <a id="s-5a72020f4a"></a>`distribution`: `stove0-target-support`
- <a id="s-8b1fa02ee9"></a>`module`: `stove0_target_support`
- <a id="s-d600325e01"></a>`name`: `from_request`
- <a id="s-22bad59c90"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-fc7bd87d61"></a>`unit`: `member`

### Declared structure

- <a id="s-68bbf14704"></a>`kind`: `"classmethod"`
- <a id="s-3c2bed23b2"></a>`signature`: `"\"(cls, request: 'TargetJobRequest', *, cancellation_check: 'CancellationCheck \| None' = None, producer_version: 'str' = 'development', session: 'TargetExecutionSession \| None' = None) -> 'TargetExecutionRuntime'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-c01d400f3a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.from_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e511d78d8c9c2f56593ddcea48631a9353aeb6c1f4e5d988a9eae765c091154a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, request: 'TargetJobRequest', *, cancellation_check: 'CancellationCheck | None' = None, producer_version: 'str' = 'development', session: 'TargetExecutionSession | None' = None) -> 'TargetExecutionRuntime'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "from_request",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```

</details>
