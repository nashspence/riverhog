# stove0_observer_protocol.ObserverDescriptor.unique_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescript-1be737f420:e72feb8860 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6af306b1a6"></a>
- <a id="s-f6e9a7664c"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-57f33dfe65"></a>`module`: `stove0_observer_protocol`
- <a id="s-94493269da"></a>`name`: `unique_contracts`
- <a id="s-c5654a5c28"></a>`owner`: `stove0_observer_protocol.ObserverDescriptor`
- <a id="s-bd24d241c5"></a>`unit`: `member`

### Declared structure

- <a id="s-f933bfb0ce"></a>`kind`: `"classmethod"`
- <a id="s-061648f0a2"></a>`signature`: `"\"(cls, value: 'tuple[ObserverContractSupport, ...]') -> 'tuple[ObserverContractSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [ObserverDescriptor](stove0-observer-protocol-observerdescriptor.md)

## Governing policies

- <a id="pa-043b28d28e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptor.unique_contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dab808dba29aee30f38421620b42dc664f85d78bbb867f43983357a650b2d7a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObserverContractSupport, ...]') -> 'tuple[ObserverContractSupport, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "unique_contracts",
  "owner": "stove0_observer_protocol.ObserverDescriptor",
  "unit": "member"
}
```

</details>
