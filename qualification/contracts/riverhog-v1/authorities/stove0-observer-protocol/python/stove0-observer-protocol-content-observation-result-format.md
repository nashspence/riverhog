# stove0_observer_protocol.CONTENT_OBSERVATION_RESULT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-content-observat-8fc59ddac3:b84f21f3a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c7edcf8201"></a>
- <a id="s-9a954859e9"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-3dc5e4fd28"></a>`module`: `stove0_observer_protocol`
- <a id="s-0631932944"></a>`name`: `CONTENT_OBSERVATION_RESULT_FORMAT`
- <a id="s-e2efa2a217"></a>`unit`: `export`

### Declared structure

- <a id="s-f4a244528a"></a>`kind`: `"constant"`
- <a id="s-5eca24872b"></a>`value`: `"stove0-observation-result/v1"`

## Governing policies

- <a id="pa-b9e4b5f5e5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.CONTENT_OBSERVATION_RESULT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4b6cb9cb6f66e5fabd4176916c960f99ce385897162557dd5c9c50d5353fcbd -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-observation-result/v1"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "CONTENT_OBSERVATION_RESULT_FORMAT",
  "unit": "export"
}
```

</details>
