# stove0_protocol.BranchSettlement.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsettlement-seal:cda774e315 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ab17c4b36"></a>
- <a id="s-41dc9dd002"></a>`distribution`: `stove0-protocol`
- <a id="s-4fcbffa40d"></a>`module`: `stove0_protocol`
- <a id="s-2343f003e0"></a>`name`: `seal`
- <a id="s-4a7de9cb31"></a>`owner`: `stove0_protocol.BranchSettlement`
- <a id="s-21e1fcdec5"></a>`unit`: `member`

### Declared structure

- <a id="s-5ff621a107"></a>`kind`: `"classmethod"`
- <a id="s-551672cbf8"></a>`signature`: `"\"(cls, *, branch: 'BranchPlan', derivation_sha256: 'str', producer_settlement_sha256: 'str', output_collection: 'CollectionRootIdentityRef', output_selection: 'ArtifactSelection') -> 'BranchSettlement'\""`

## Maintained corroboration

### Related interface records

- [BranchSettlement](stove0-protocol-branchsettlement.md)

## Governing policies

- <a id="pa-ea6a5b6696"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSettlement.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8f602a8871611f28d7d38a921d120ef1d275ab3eb3be483c71ca741fc09c3e6 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, branch: 'BranchPlan', derivation_sha256: 'str', producer_settlement_sha256: 'str', output_collection: 'CollectionRootIdentityRef', output_selection: 'ArtifactSelection') -> 'BranchSettlement'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.BranchSettlement",
  "unit": "member"
}
```

</details>
