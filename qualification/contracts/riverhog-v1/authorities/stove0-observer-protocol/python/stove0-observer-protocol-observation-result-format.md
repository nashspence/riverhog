# stove0_observer_protocol.OBSERVATION_RESULT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observation-result-format:84d421bfcd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db7ee34557"></a>
- <a id="s-67e88d2717"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-de75de7124"></a>`module`: `stove0_observer_protocol`
- <a id="s-311253dbb4"></a>`name`: `OBSERVATION_RESULT_FORMAT`
- <a id="s-0eb816e575"></a>`unit`: `export`

### Declared structure

- <a id="s-2df0d8f7cd"></a>`kind`: `"constant"`
- <a id="s-0e5cf4306e"></a>`value`: `"stove0-observation-result/v1"`

## Governing policies

- <a id="pa-312d8b2ee6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.OBSERVATION_RESULT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8ded7c64746ea63a6b30e08666f393a70d94a9e71b963a346598398cc8f2c9f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-observation-result/v1"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "OBSERVATION_RESULT_FORMAT",
  "unit": "export"
}
```

</details>
