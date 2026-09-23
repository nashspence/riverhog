# stove0_observer_protocol.ObserverContract.bind_semantic_conformance_vectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract-e3f6d2adb1:de87140f1f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ed99e0e4d"></a>
- <a id="s-4f4c164f4b"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-a2e4e28148"></a>`module`: `stove0_observer_protocol`
- <a id="s-9a2113d1a6"></a>`name`: `bind_semantic_conformance_vectors`
- <a id="s-921af86598"></a>`owner`: `stove0_observer_protocol.ObserverContract`
- <a id="s-5ebcb45713"></a>`unit`: `member`

### Declared structure

- <a id="s-4d33d78ad1"></a>`kind`: `"method"`
- <a id="s-e42414046d"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ObserverContract](stove0-observer-protocol-observercontract.md)

## Governing policies

- <a id="pa-ebdfb9c283"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContract.bind_semantic_conformance_vectors`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c2a58bb7810fa32cc01f24703c4aae6266bdc7636396e60be5def591e6cfe19a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "bind_semantic_conformance_vectors",
  "owner": "stove0_observer_protocol.ObserverContract",
  "unit": "member"
}
```

</details>
