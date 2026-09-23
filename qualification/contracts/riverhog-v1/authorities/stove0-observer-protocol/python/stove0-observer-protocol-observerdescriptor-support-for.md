# stove0_observer_protocol.ObserverDescriptor.support_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescript-c431d12733:3625e89de1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-605de82a94"></a>
- <a id="s-ee5c6a4cbd"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-e50778beeb"></a>`module`: `stove0_observer_protocol`
- <a id="s-73868d4e7f"></a>`name`: `support_for`
- <a id="s-e8245d0f60"></a>`owner`: `stove0_observer_protocol.ObserverDescriptor`
- <a id="s-04ca8a06c8"></a>`unit`: `member`

### Declared structure

- <a id="s-1b79c007ea"></a>`kind`: `"method"`
- <a id="s-e935bc1497"></a>`signature`: `"\"(self, contract_id: 'str') -> 'ObserverContractSupport'\""`

## Maintained corroboration

### Related interface records

- [ObserverDescriptor](stove0-observer-protocol-observerdescriptor.md)

## Governing policies

- <a id="pa-f8d751fc34"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptor.support_for`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5528271267ff9373083495966e2baaba9edc69cef1c98f0101a88dba56ab65e4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, contract_id: 'str') -> 'ObserverContractSupport'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "support_for",
  "owner": "stove0_observer_protocol.ObserverDescriptor",
  "unit": "member"
}
```

</details>
