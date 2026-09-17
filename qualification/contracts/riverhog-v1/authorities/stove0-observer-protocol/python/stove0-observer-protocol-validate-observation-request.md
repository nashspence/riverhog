# stove0_observer_protocol.validate_observation_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-validate-observa-2e2c0d29b3:99cc541c28 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c874800783"></a>
- <a id="s-650ddce070"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-fdfe854ea7"></a>`module`: `stove0_observer_protocol`
- <a id="s-761d43d9d8"></a>`name`: `validate_observation_request`
- <a id="s-8c02f9c964"></a>`unit`: `export`

### Declared structure

- <a id="s-52e55afc51"></a>`kind`: `"function"`
- <a id="s-4ce8185602"></a>`signature`: `"\"(request: 'ObservationRequest', descriptor: 'ObserverDescriptor') -> 'ObserverContractSupport'\""`

## Governing policies

- <a id="pa-9e13417ab6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.validate_observation_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a26e23a4dda27d23d8dcc6eeafaf324945156b467a23de50cdf10d89d74d697e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ObservationRequest', descriptor: 'ObserverDescriptor') -> 'ObserverContractSupport'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "validate_observation_request",
  "unit": "export"
}
```

</details>
