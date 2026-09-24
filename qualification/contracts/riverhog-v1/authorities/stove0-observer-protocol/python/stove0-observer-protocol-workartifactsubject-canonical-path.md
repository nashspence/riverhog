# stove0_observer_protocol.WorkArtifactSubject.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-workartifactsubj-4e03be8978:d08424af2a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-73e4fc0d4b"></a>
- <a id="s-403251899f"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-7eb26bc8f9"></a>`module`: `stove0_observer_protocol`
- <a id="s-eb4343bd88"></a>`name`: `canonical_path`
- <a id="s-d8cef81845"></a>`owner`: `stove0_observer_protocol.WorkArtifactSubject`
- <a id="s-fd305c934e"></a>`unit`: `member`

### Declared structure

- <a id="s-eae36044d0"></a>`kind`: `"classmethod"`
- <a id="s-bc857ba406"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [WorkArtifactSubject](stove0-observer-protocol-workartifactsubject.md)

## Governing policies

- <a id="pa-a46168a6be"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.WorkArtifactSubject.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdae5cd8c0f29fa12b9d499616150c6111c95637a6a490ea1a1a6480d228edcc -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_path",
  "owner": "stove0_observer_protocol.WorkArtifactSubject",
  "unit": "member"
}
```

</details>
