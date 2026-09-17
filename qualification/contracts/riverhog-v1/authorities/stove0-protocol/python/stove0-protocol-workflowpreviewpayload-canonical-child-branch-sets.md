# stove0_protocol.WorkflowPreviewPayload.canonical_child_branch_sets

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-ca-aa80a554f2:9161e36c92 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8478119f07"></a>
- <a id="s-a4f90a4a23"></a>`distribution`: `stove0-protocol`
- <a id="s-a475804a42"></a>`module`: `stove0_protocol`
- <a id="s-86f6b61933"></a>`name`: `canonical_child_branch_sets`
- <a id="s-5aa29a4e56"></a>`owner`: `stove0_protocol.WorkflowPreviewPayload`
- <a id="s-dd5746b99b"></a>`unit`: `member`

### Declared structure

- <a id="s-cb274c205b"></a>`kind`: `"classmethod"`
- <a id="s-14a3a192b2"></a>`signature`: `"\"(cls, value: 'tuple[BranchSetPlan, ...]') -> 'tuple[BranchSetPlan, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-fa809e42d5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.canonical_child_branch_sets`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: febdfd5eb1334433ef9282f6d9decddaa481dd5e41994ccd2060cdb1b5f3f52f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[BranchSetPlan, ...]') -> 'tuple[BranchSetPlan, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_child_branch_sets",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```

</details>
