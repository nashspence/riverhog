# stove0_protocol.ArtifactSubject.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactsubject-canonical-path:e11a991a54 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae85909825"></a>
- <a id="s-4d83b9be70"></a>`distribution`: `stove0-protocol`
- <a id="s-9db243be12"></a>`module`: `stove0_protocol`
- <a id="s-cb8b31d25d"></a>`name`: `canonical_path`
- <a id="s-978635aab9"></a>`owner`: `stove0_protocol.ArtifactSubject`
- <a id="s-62c8649765"></a>`unit`: `member`

### Declared structure

- <a id="s-74689da1f9"></a>`kind`: `"classmethod"`
- <a id="s-36e7a0f1b6"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSubject](stove0-protocol-artifactsubject.md)

## Governing policies

- <a id="pa-b4e93cc0c5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSubject.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08e4ab08fbb37de69fb82d5f1f8b49550c01cbf69d152d34258bdc9d69611a51 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_path",
  "owner": "stove0_protocol.ArtifactSubject",
  "unit": "member"
}
```

</details>
