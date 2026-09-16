# stove0_target_support.TargetExecutionRuntime.declare_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-e9f0baf66e:e0daf00a73 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-16420883c3"></a>
- <a id="s-6c15f6de4f"></a>`distribution`: `stove0-target-support`
- <a id="s-51f3b10f3b"></a>`module`: `stove0_target_support`
- <a id="s-85704bc5d8"></a>`name`: `declare_disposition`
- <a id="s-7730075421"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-d4b87f480f"></a>`unit`: `member`

### Declared structure

- <a id="s-3c20cef201"></a>`kind`: `"method"`
- <a id="s-b6cd7415b1"></a>`signature`: `"\"(self, input_id: 'str', status: 'InputDisposition') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-d0c797a246"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.declare_disposition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e570f8ba682300d38742e4b72c048c677bb78d452f9d29f49ef1a06ed9624eed -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, input_id: 'str', status: 'InputDisposition') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "declare_disposition",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```

</details>
