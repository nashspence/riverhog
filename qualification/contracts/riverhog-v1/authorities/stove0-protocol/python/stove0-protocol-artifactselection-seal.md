# stove0_protocol.ArtifactSelection.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection-seal:744ee394fb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46823cf6a5"></a>
- <a id="s-ce6aad21be"></a>`distribution`: `stove0-protocol`
- <a id="s-ec30378d13"></a>`module`: `stove0_protocol`
- <a id="s-0767052ba7"></a>`name`: `seal`
- <a id="s-26c94e700f"></a>`owner`: `stove0_protocol.ArtifactSelection`
- <a id="s-a29898de5b"></a>`unit`: `member`

### Declared structure

- <a id="s-9893ae0fda"></a>`kind`: `"classmethod"`
- <a id="s-be8fb02b8c"></a>`signature`: `"\"(cls, artifacts: 'Sequence[ArtifactSubject]') -> 'ArtifactSelection'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSelection](stove0-protocol-artifactselection.md)

## Governing policies

- <a id="pa-7cdb605b36"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82335920d8a6e59b1b7412de8ee6e1ce62329bac131da9eb27ce9f23f40b173e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, artifacts: 'Sequence[ArtifactSubject]') -> 'ArtifactSelection'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.ArtifactSelection",
  "unit": "member"
}
```

</details>
