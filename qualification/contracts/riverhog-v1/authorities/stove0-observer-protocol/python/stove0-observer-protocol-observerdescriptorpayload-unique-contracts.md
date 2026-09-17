# stove0_observer_protocol.ObserverDescriptorPayload.unique_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescript-4f81732a36:b396dd5b28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-74dbcd74c6"></a>
- <a id="s-e116c7ebef"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-480d6fdb38"></a>`module`: `stove0_observer_protocol`
- <a id="s-87308674ec"></a>`name`: `unique_contracts`
- <a id="s-c2650c4796"></a>`owner`: `stove0_observer_protocol.ObserverDescriptorPayload`
- <a id="s-b18dfbdd7e"></a>`unit`: `member`

### Declared structure

- <a id="s-32eaf57a36"></a>`kind`: `"classmethod"`
- <a id="s-9148fca75b"></a>`signature`: `"\"(cls, value: 'tuple[ObserverContractSupport, ...]') -> 'tuple[ObserverContractSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [ObserverDescriptorPayload](stove0-observer-protocol-observerdescriptorpayload.md)

## Governing policies

- <a id="pa-11a13f4d5d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptorPayload.unique_contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e246fc91fc0c26e8f173c9fb36e65a2089587ad5bd79dda31f1e606b16038d42 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObserverContractSupport, ...]') -> 'tuple[ObserverContractSupport, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "unique_contracts",
  "owner": "stove0_observer_protocol.ObserverDescriptorPayload",
  "unit": "member"
}
```

</details>
