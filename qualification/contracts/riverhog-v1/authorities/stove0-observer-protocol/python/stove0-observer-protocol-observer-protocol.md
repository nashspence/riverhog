# stove0_observer_protocol.OBSERVER_PROTOCOL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observer-protocol:acb4456956 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef5ec9ee9f"></a>
- <a id="s-3d7a823d1b"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-455b66df49"></a>`module`: `stove0_observer_protocol`
- <a id="s-c1c368f1d9"></a>`name`: `OBSERVER_PROTOCOL`
- <a id="s-79e0fe0d5e"></a>`unit`: `export`

### Declared structure

- <a id="s-7d2b2d39b7"></a>`kind`: `"constant"`
- <a id="s-b7fec49017"></a>`value`: `"stove0-content-observer/v1"`

## Governing policies

- <a id="pa-12954572f5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.OBSERVER_PROTOCOL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30a5535885af4876896b5818208be0ee37bc3f8c5c73978047f51d5cef60bb63 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-content-observer/v1"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "OBSERVER_PROTOCOL",
  "unit": "export"
}
```

</details>
