# stove0_observer_protocol.ContentObservationRequestPayload.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-fcbdc7ccfd:1a985fcb2d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-174028d232"></a>
- <a id="s-630790646a"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-43e3ea48b7"></a>`module`: `stove0_observer_protocol`
- <a id="s-6f58fcffb9"></a>`name`: `canonical_subjects`
- <a id="s-18343651e7"></a>`owner`: `stove0_observer_protocol.ContentObservationRequestPayload`
- <a id="s-1475548363"></a>`unit`: `member`

### Declared structure

- <a id="s-b57c1832d2"></a>`kind`: `"classmethod"`
- <a id="s-6782b73545"></a>`signature`: `"\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRequestPayload](stove0-observer-protocol-contentobservationrequestpayload.md)

## Governing policies

- <a id="pa-8bc03b2184"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationRequestPayload.canonical_subjects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72abd96c7151f9a288ac8913a3c0563b617b319483d47e7d7957a465eca752c9 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ContentObservationRequestPayload",
  "unit": "member"
}
```

</details>
