# stove0_protocol.BranchSetDecision.canonical_selections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetdecision-canonic-eed2c68113:a49aa06873 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-908b44e890"></a>
- <a id="s-7894793965"></a>`distribution`: `stove0-protocol`
- <a id="s-d3162fd4b8"></a>`module`: `stove0_protocol`
- <a id="s-873f84267e"></a>`name`: `canonical_selections`
- <a id="s-f4359d21df"></a>`owner`: `stove0_protocol.BranchSetDecision`
- <a id="s-8c71abee1c"></a>`unit`: `member`

### Declared structure

- <a id="s-6a96bd09aa"></a>`kind`: `"classmethod"`
- <a id="s-ee6ae29259"></a>`signature`: `"\"(cls, value: 'tuple[ArtifactSelection, ...]') -> 'tuple[ArtifactSelection, ...]'\""`

## Maintained corroboration

### Related interface records

- [BranchSetDecision](stove0-protocol-branchsetdecision.md)

## Governing policies

- <a id="pa-c49e2da1e7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetDecision.canonical_selections`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 646064950e584934ba200da14905e67d306d1c75b7c83dbf6f798ffd538d3238 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSelection, ...]') -> 'tuple[ArtifactSelection, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_selections",
  "owner": "stove0_protocol.BranchSetDecision",
  "unit": "member"
}
```

</details>
