# stove0_target_protocol.TargetContractPayload.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontractpayl-6716882bb5:5fd35b270e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d7e3ec84a"></a>
- <a id="s-5559364445"></a>`distribution`: `stove0-target-protocol`
- <a id="s-b356983247"></a>`module`: `stove0_target_protocol`
- <a id="s-6a2de9962a"></a>`name`: `canonical_operations`
- <a id="s-0b13be0c6f"></a>`owner`: `stove0_target_protocol.TargetContractPayload`
- <a id="s-70da61dcfd"></a>`unit`: `member`

### Declared structure

- <a id="s-77da8dcb46"></a>`kind`: `"classmethod"`
- <a id="s-caacac135a"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetContractPayload](stove0-target-protocol-targetcontractpayload.md)

## Governing policies

- <a id="pa-a7a21853bc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContractPayload.canonical_operations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b5084933763eb034d804a60cff57aaafdf4cfa5f405f6f0614d6d83bfac86b2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_operations",
  "owner": "stove0_target_protocol.TargetContractPayload",
  "unit": "member"
}
```

</details>
