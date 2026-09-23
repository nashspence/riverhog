# stove0_observer_protocol.ArtifactSubject.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-artifactsubject-5961e4565a:76ff785cff -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a1322510f"></a>
- <a id="s-9b467be08f"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-05c63e89f0"></a>`module`: `stove0_observer_protocol`
- <a id="s-5b7b3e3aba"></a>`name`: `canonical_path`
- <a id="s-c9307af696"></a>`owner`: `stove0_observer_protocol.ArtifactSubject`
- <a id="s-3015f23db5"></a>`unit`: `member`

### Declared structure

- <a id="s-887810c2dc"></a>`kind`: `"classmethod"`
- <a id="s-c83819a4dc"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSubject](stove0-observer-protocol-artifactsubject.md)

## Governing policies

- <a id="pa-55747ac0bc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ArtifactSubject.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f89c199602cdc663eb9ff224699822ae18d8d440d585d24863ab3ae9fa837a5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_path",
  "owner": "stove0_observer_protocol.ArtifactSubject",
  "unit": "member"
}
```

</details>
