# stove0_protocol.JOIN_PLAN_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-join-plan-format:1a88fe5a5c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34fde5b1a5"></a>
- <a id="s-7c58130c84"></a>`distribution`: `stove0-protocol`
- <a id="s-a3521b806c"></a>`module`: `stove0_protocol`
- <a id="s-47c824c04a"></a>`name`: `JOIN_PLAN_FORMAT`
- <a id="s-e2d49d6fad"></a>`unit`: `export`

### Declared structure

- <a id="s-8ead7c0ada"></a>`kind`: `"constant"`
- <a id="s-3e499d4379"></a>`value`: `"stove0-join-plan/v1"`

## Governing policies

- <a id="pa-b4acc2f66e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JOIN_PLAN_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 275466f3d49747822e3919ab94955f333dd05ad9961acaa9bae825c2582596a1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-join-plan/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JOIN_PLAN_FORMAT",
  "unit": "export"
}
```

</details>
