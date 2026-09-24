# stove0_protocol.ArtifactSelection.canonical_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection-canonic-bce31f53f0:3a8cd33b4d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef143318c0"></a>
- <a id="s-1039f10c91"></a>`distribution`: `stove0-protocol`
- <a id="s-14f86255d3"></a>`module`: `stove0_protocol`
- <a id="s-ce74741b00"></a>`name`: `canonical_artifacts`
- <a id="s-aca1184524"></a>`owner`: `stove0_protocol.ArtifactSelection`
- <a id="s-053e4044a7"></a>`unit`: `member`

### Declared structure

- <a id="s-eb854192ba"></a>`kind`: `"classmethod"`
- <a id="s-167bbc2650"></a>`signature`: `"\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSelection](stove0-protocol-artifactselection.md)

## Governing policies

- <a id="pa-063280061d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection.canonical_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21c7f91a18c1fe069a7fe724c26662962a33b321f17b133b223c6a2a06ef72b2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_artifacts",
  "owner": "stove0_protocol.ArtifactSelection",
  "unit": "member"
}
```

</details>
