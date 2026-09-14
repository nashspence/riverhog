# stove0_protocol.ArtifactSelectionRef.from_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselectionref-from-selection:bb4ef782b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-08439237ad"></a>
- <a id="s-4d17349e4b"></a>`distribution`: `stove0-protocol`
- <a id="s-3f2a178e3e"></a>`module`: `stove0_protocol`
- <a id="s-82beafc96f"></a>`name`: `from_selection`
- <a id="s-ae3a746090"></a>`owner`: `stove0_protocol.ArtifactSelectionRef`
- <a id="s-c93beb2379"></a>`unit`: `member`

### Declared structure

- <a id="s-30d3f114e0"></a>`kind`: `"classmethod"`
- <a id="s-be68def38b"></a>`signature`: `"\"(cls, selection: 'ArtifactSelection') -> 'ArtifactSelectionRef'\""`

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelectionRef](stove0-protocol-artifactselectionref.md)

## Governing policies

- <a id="pa-76cd12cf3b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelectionRef.from_selection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92c7674ceb527c57c34213e0956d9b38cdd32570b405666d9af13a495ab20145 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, selection: 'ArtifactSelection') -> 'ArtifactSelectionRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_selection",
  "owner": "stove0_protocol.ArtifactSelectionRef",
  "unit": "member"
}
```
