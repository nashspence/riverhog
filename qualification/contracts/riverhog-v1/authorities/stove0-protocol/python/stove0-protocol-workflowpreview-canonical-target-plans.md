# stove0_protocol.WorkflowPreview.canonical_target_plans

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview-canonical-9d356a0cd1:ef91df5773 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-28a6da7654"></a>
- <a id="s-bc7ea91498"></a>`distribution`: `stove0-protocol`
- <a id="s-40faf6c029"></a>`module`: `stove0_protocol`
- <a id="s-72913de554"></a>`name`: `canonical_target_plans`
- <a id="s-ea3a44a371"></a>`owner`: `stove0_protocol.WorkflowPreview`
- <a id="s-d24f37ce65"></a>`unit`: `member`

### Declared structure

- <a id="s-f5be51c5d8"></a>`kind`: `"classmethod"`
- <a id="s-46d4f5b32f"></a>`signature`: `"\"(cls, value: 'tuple[BranchTargetPreview, ...]') -> 'tuple[BranchTargetPreview, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreview](stove0-protocol-workflowpreview.md)

## Governing policies

- <a id="pa-a29742ddb4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview.canonical_target_plans`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ed5c07fe9abae734872afe4702f7919f6a24e4fd24532875a908c6ff3c479c1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[BranchTargetPreview, ...]') -> 'tuple[BranchTargetPreview, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_target_plans",
  "owner": "stove0_protocol.WorkflowPreview",
  "unit": "member"
}
```

</details>
