# stove0_observer_protocol.OBSERVATION_REQUEST_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observation-request-format:9e16481711 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d060a7f5f9"></a>
- <a id="s-7eea30ad40"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-d03d15f113"></a>`module`: `stove0_observer_protocol`
- <a id="s-0d01d4f2c4"></a>`name`: `OBSERVATION_REQUEST_FORMAT`
- <a id="s-329534f06e"></a>`unit`: `export`

### Declared structure

- <a id="s-a18e0b5cc5"></a>`kind`: `"constant"`
- <a id="s-1d821905ff"></a>`value`: `"stove0-observation-request/v1"`

## Governing policies

- <a id="pa-fc13d4e6dd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.OBSERVATION_REQUEST_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1cf764bb6b5f21f6d0db0a911b4a0a3266895ebf0d42d2a0084380f1882ea8f2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-observation-request/v1"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "OBSERVATION_REQUEST_FORMAT",
  "unit": "export"
}
```

</details>
