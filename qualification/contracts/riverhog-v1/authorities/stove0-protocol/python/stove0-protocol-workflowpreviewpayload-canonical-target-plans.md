# stove0_protocol.WorkflowPreviewPayload.canonical_target_plans

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-ca-3d3dd5a5be:d8a7b87224 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8510c090fc"></a>
- <a id="s-7f1a443356"></a>`distribution`: `stove0-protocol`
- <a id="s-7292a08ea3"></a>`module`: `stove0_protocol`
- <a id="s-c9b6238ccc"></a>`name`: `canonical_target_plans`
- <a id="s-a8252df979"></a>`owner`: `stove0_protocol.WorkflowPreviewPayload`
- <a id="s-5b22a9a868"></a>`unit`: `member`

### Declared structure

- <a id="s-8b6f73c8f6"></a>`kind`: `"classmethod"`
- <a id="s-db9ffa37ab"></a>`signature`: `"\"(cls, value: 'tuple[BranchTargetPreview, ...]') -> 'tuple[BranchTargetPreview, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-d1e091af60"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.canonical_target_plans`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf3e9fa3cf3bfe83b79cc82765beeb419c4c1356d9839f248904397f01eea4d1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[BranchTargetPreview, ...]') -> 'tuple[BranchTargetPreview, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_target_plans",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```

</details>
