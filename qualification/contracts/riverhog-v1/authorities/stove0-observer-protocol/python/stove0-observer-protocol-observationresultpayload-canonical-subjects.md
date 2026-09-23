# stove0_observer_protocol.ObservationResultPayload.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationresul-6ed3493064:b1a917fb48 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ddfe0b9d0c"></a>
- <a id="s-68176dd718"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-a9e97d3ace"></a>`module`: `stove0_observer_protocol`
- <a id="s-4dd39c4937"></a>`name`: `canonical_subjects`
- <a id="s-635a287516"></a>`owner`: `stove0_observer_protocol.ObservationResultPayload`
- <a id="s-d985de1abe"></a>`unit`: `member`

### Declared structure

- <a id="s-bd485bddcc"></a>`kind`: `"classmethod"`
- <a id="s-4b34eacb48"></a>`signature`: `"\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ObservationResultPayload](stove0-observer-protocol-observationresultpayload.md)

## Governing policies

- <a id="pa-502c130399"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationResultPayload.canonical_subjects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 810189a533d3daa10471fb734bbfa8c8ea66db80563a46ceb3cebf12d453c244 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ObservationResultPayload",
  "unit": "member"
}
```

</details>
