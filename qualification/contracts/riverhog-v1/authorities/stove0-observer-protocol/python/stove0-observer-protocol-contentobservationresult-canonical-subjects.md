# stove0_observer_protocol.ContentObservationResult.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-f7ea83cfc1:e33b108ac7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3bc98a8499"></a>
- <a id="s-8d8edaf004"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-897e7463d7"></a>`module`: `stove0_observer_protocol`
- <a id="s-8e0b2a1a70"></a>`name`: `canonical_subjects`
- <a id="s-d07a1b76f0"></a>`owner`: `stove0_observer_protocol.ContentObservationResult`
- <a id="s-4932bffcf5"></a>`unit`: `member`

### Declared structure

- <a id="s-5de1538178"></a>`kind`: `"classmethod"`
- <a id="s-7487d14d97"></a>`signature`: `"\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResult](stove0-observer-protocol-contentobservationresult.md)

## Governing policies

- <a id="pa-a24ea307e5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationResult.canonical_subjects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 834008cb458be52d384721d6c557f1776a53c8819c5f67cdefc757f53ad673aa -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[WorkArtifactSubject, ...]') -> 'tuple[WorkArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ContentObservationResult",
  "unit": "member"
}
```

</details>
