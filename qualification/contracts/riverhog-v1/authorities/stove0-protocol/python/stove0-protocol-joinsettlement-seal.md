# stove0_protocol.JoinSettlement.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinsettlement-seal:70fd7ce070 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1c15242147"></a>
- <a id="s-241b2c06fb"></a>`distribution`: `stove0-protocol`
- <a id="s-eadf40e3fd"></a>`module`: `stove0_protocol`
- <a id="s-d301d70284"></a>`name`: `seal`
- <a id="s-e3073ae6fa"></a>`owner`: `stove0_protocol.JoinSettlement`
- <a id="s-3a26c3d5c4"></a>`unit`: `member`

### Declared structure

- <a id="s-04065caee6"></a>`kind`: `"classmethod"`
- <a id="s-d325b1671d"></a>`signature`: `"\"(cls, *, plan: 'JoinPlan', derivation_sha256: 'str', producer_settlement_sha256: 'str', output_collection: 'CollectionRootRef', output_selection: 'ArtifactSelection') -> 'JoinSettlement'\""`

## Maintained corroboration

### Related interface records

- [JoinSettlement](stove0-protocol-joinsettlement.md)

## Governing policies

- <a id="pa-62048686dc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinSettlement.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 536f735fca0d36a9a622963b69103a98bd973b389871bfab42d1cb513da69feb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, plan: 'JoinPlan', derivation_sha256: 'str', producer_settlement_sha256: 'str', output_collection: 'CollectionRootRef', output_selection: 'ArtifactSelection') -> 'JoinSettlement'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.JoinSettlement",
  "unit": "member"
}
```

</details>
