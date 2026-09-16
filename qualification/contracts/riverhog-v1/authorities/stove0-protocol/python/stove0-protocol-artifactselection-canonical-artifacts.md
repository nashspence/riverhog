# stove0_protocol.ArtifactSelection.canonical_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection-canonic-bce31f53f0:3a8cd33b4d -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-167bbc2650"></a>`signature`: `"\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSelection](stove0-protocol-artifactselection.md)

## Governing policies

- <a id="pa-063280061d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection.canonical_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6e67b798dae9fc9e520123357e23a811c3ba6e1d287547b0058fab8c787b8f1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_artifacts",
  "owner": "stove0_protocol.ArtifactSelection",
  "unit": "member"
}
```

</details>
