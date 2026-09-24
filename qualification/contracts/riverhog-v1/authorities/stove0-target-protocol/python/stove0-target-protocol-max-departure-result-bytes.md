# stove0_target_protocol.MAX_DEPARTURE_RESULT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-max-departure-result-bytes:357e355773 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5db3eccc87"></a>
- <a id="s-3f170b26f7"></a>`distribution`: `stove0-target-protocol`
- <a id="s-df6cc5f62f"></a>`module`: `stove0_target_protocol`
- <a id="s-c6aa0a6bf0"></a>`name`: `MAX_DEPARTURE_RESULT_BYTES`
- <a id="s-07cfb28bc0"></a>`unit`: `export`

### Declared structure

- <a id="s-67acb83dd1"></a>`kind`: `"constant"`
- <a id="s-00f55e51f1"></a>`value`: `65536`

## Governing policies

- <a id="pa-5e8f176d9a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.MAX_DEPARTURE_RESULT_BYTES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d356af3b865f6a63e86bae31b45651416c5ba910aa44932d54352b83c69c184 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 65536
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "MAX_DEPARTURE_RESULT_BYTES",
  "unit": "export"
}
```

</details>
