# stove0_protocol.WorkArtifactSubject.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workartifactsubject-canonical-path:54f0cbb44e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4903e68b1"></a>
- <a id="s-c3fc328a2a"></a>`distribution`: `stove0-protocol`
- <a id="s-0435a70319"></a>`module`: `stove0_protocol`
- <a id="s-d0b3278e48"></a>`name`: `canonical_path`
- <a id="s-efc28a78b9"></a>`owner`: `stove0_protocol.WorkArtifactSubject`
- <a id="s-f495caf9e0"></a>`unit`: `member`

### Declared structure

- <a id="s-85bdee4776"></a>`kind`: `"classmethod"`
- <a id="s-2f3f2941d9"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [WorkArtifactSubject](stove0-protocol-workartifactsubject.md)

## Governing policies

- <a id="pa-99edb35217"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkArtifactSubject.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cfe3b384db89501a739665ebbfc96f06ed4f754feb6b871be7edc527b16c1d4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_path",
  "owner": "stove0_protocol.WorkArtifactSubject",
  "unit": "member"
}
```

</details>
