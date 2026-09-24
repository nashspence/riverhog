# stove0_observer_protocol.ContentObservationResultPayload.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-2b355f43a7:c3fe299f19 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d446f97a5"></a>
- <a id="s-6fc99deafd"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-8775f8b26c"></a>`module`: `stove0_observer_protocol`
- <a id="s-0ee956787c"></a>`name`: `canonical_subjects`
- <a id="s-51b1a4a33c"></a>`owner`: `stove0_observer_protocol.ContentObservationResultPayload`
- <a id="s-1779517101"></a>`unit`: `member`

### Declared structure

- <a id="s-e164fd48c1"></a>`kind`: `"classmethod"`
- <a id="s-c521b4b468"></a>`signature`: `"\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResultPayload](stove0-observer-protocol-contentobservationresultpayload.md)

## Governing policies

- <a id="pa-f0c0b162a2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationResultPayload.canonical_subjects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b01d5e09ceba69745109a3a0431c70056a7e76bf3c48d075bfe05443f4fe1390 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ContentObservationResultPayload",
  "unit": "member"
}
```

</details>
