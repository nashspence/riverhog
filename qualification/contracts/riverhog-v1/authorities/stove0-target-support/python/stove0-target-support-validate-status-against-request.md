# stove0_target_support.validate_status_against_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-validate-status-aga-cc3e5704d3:83a3110237 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2584d80833"></a>
- <a id="s-57e4b0d70b"></a>`distribution`: `stove0-target-support`
- <a id="s-95996a4bb0"></a>`module`: `stove0_target_support`
- <a id="s-3ba9ea8e83"></a>`name`: `validate_status_against_request`
- <a id="s-2d53e05e7d"></a>`unit`: `export`

### Declared structure

- <a id="s-2223359903"></a>`kind`: `"function"`
- <a id="s-2d86d3a4f8"></a>`signature`: `"\"(status: 'TargetJobStatus', request: 'TargetJobRequest \| AcceptedTargetJob', operation: 'OperationContract') -> 'None'\""`

## Governing policies

- <a id="pa-ed72e298fa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.validate_status_against_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb64d44531558f7656fc720458c234126afc4ebd2598a0ca9472d1c2b1fd2150 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(status: 'TargetJobStatus', request: 'TargetJobRequest | AcceptedTargetJob', operation: 'OperationContract') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_status_against_request",
  "unit": "export"
}
```

</details>
