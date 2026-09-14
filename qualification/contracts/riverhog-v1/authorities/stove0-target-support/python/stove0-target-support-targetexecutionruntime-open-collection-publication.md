# stove0_target_support.TargetExecutionRuntime.open_collection_publication

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-64f503421a:23889353bc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f6f85a41a"></a>
- <a id="s-7e5efc1b86"></a>`distribution`: `stove0-target-support`
- <a id="s-bd4f12b129"></a>`module`: `stove0_target_support`
- <a id="s-1f029ca8bc"></a>`name`: `open_collection_publication`
- <a id="s-7424e83e05"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-564253ba6a"></a>`unit`: `member`

### Declared structure

- <a id="s-fa0878f402"></a>`kind`: `"method"`
- <a id="s-32b766ceba"></a>`signature`: `"\"(self, *, source_context: 'Mapping[str, object] \| None' = None) -> 'TargetCollectionPublication'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-b29b513ae5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.open_collection_publication`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 508f5fef77c704c3d08567bb5ef7fc3f36c6a1fe3d275992fb3f48d2b9dd6032 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, source_context: 'Mapping[str, object] | None' = None) -> 'TargetCollectionPublication'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "open_collection_publication",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```
