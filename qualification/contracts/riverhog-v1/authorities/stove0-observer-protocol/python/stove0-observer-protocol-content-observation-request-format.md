# stove0_observer_protocol.CONTENT_OBSERVATION_REQUEST_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-content-observat-7536124b00:b7277723d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4af7aa223a"></a>
- <a id="s-2ae398f421"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-8deacb7d9f"></a>`module`: `stove0_observer_protocol`
- <a id="s-ba651cc871"></a>`name`: `CONTENT_OBSERVATION_REQUEST_FORMAT`
- <a id="s-b24ab33c55"></a>`unit`: `export`

### Declared structure

- <a id="s-b46a27620f"></a>`kind`: `"constant"`
- <a id="s-2e58fc064b"></a>`value`: `"stove0-observation-request/v1"`

## Governing policies

- <a id="pa-4c576f0fea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.CONTENT_OBSERVATION_REQUEST_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14b5cf4587ffe1c652d607d0a671d3745af5fcda2943c7815ed4b0097f707310 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-observation-request/v1"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "CONTENT_OBSERVATION_REQUEST_FORMAT",
  "unit": "export"
}
```

</details>
