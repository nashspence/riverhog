# stove0_observer_protocol.ContentObservationRequest.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-8891e0cc8c:17b3e07356 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ff8cbdcad"></a>
- <a id="s-ed7f4f94af"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-24753f1628"></a>`module`: `stove0_observer_protocol`
- <a id="s-563e09c8fe"></a>`name`: `canonical_subjects`
- <a id="s-7ff853b2dd"></a>`owner`: `stove0_observer_protocol.ContentObservationRequest`
- <a id="s-4ce3e25866"></a>`unit`: `member`

### Declared structure

- <a id="s-cf7575f250"></a>`kind`: `"classmethod"`
- <a id="s-c22fc3ddb4"></a>`signature`: `"\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRequest](stove0-observer-protocol-contentobservationrequest.md)

## Governing policies

- <a id="pa-93e831ad40"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationRequest.canonical_subjects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aff8b963c795873b68fcbabe7c76b97d1d5ef2995984ceff3b69c55ff5669e33 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ContentObservationRequest",
  "unit": "member"
}
```

</details>
