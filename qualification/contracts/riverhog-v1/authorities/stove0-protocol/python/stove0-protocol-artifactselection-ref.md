# stove0_protocol.ArtifactSelection.ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection-ref:0d2bac72ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-93fb2992d3"></a>
- <a id="s-f05c0176d8"></a>`distribution`: `stove0-protocol`
- <a id="s-e5724a3a1f"></a>`module`: `stove0_protocol`
- <a id="s-128fb37684"></a>`name`: `ref`
- <a id="s-c9d2e401f7"></a>`owner`: `stove0_protocol.ArtifactSelection`
- <a id="s-062e5e1a91"></a>`unit`: `member`

### Declared structure

- <a id="s-6079867e87"></a>`kind`: `"method"`
- <a id="s-12c8d7d2da"></a>`signature`: `"\"(self) -> 'ArtifactSelectionRef'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSelection](stove0-protocol-artifactselection.md)

## Governing policies

- <a id="pa-7dbd4c088a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection.ref`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bcb7e55483cffc0190a8069983003099f46a6a5b6201eddeffe70b1156f29067 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ArtifactSelectionRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ref",
  "owner": "stove0_protocol.ArtifactSelection",
  "unit": "member"
}
```
