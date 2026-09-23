# stove0_target_support.DEFAULT_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-default-terminal-st-74c58e171e:c178396db7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-78e4358cbf"></a>
- <a id="s-1bc5f837e9"></a>`distribution`: `stove0-target-support`
- <a id="s-0c809877f8"></a>`module`: `stove0_target_support`
- <a id="s-68a5c4a176"></a>`name`: `DEFAULT_TERMINAL_STATE_RETENTION_SECONDS`
- <a id="s-c383d2e813"></a>`unit`: `export`

### Declared structure

- <a id="s-20562318ba"></a>`kind`: `"constant"`
- <a id="s-b38dede4d2"></a>`value`: `2592000`

## Governing policies

- <a id="pa-4e8c645e8c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.DEFAULT_TERMINAL_STATE_RETENTION_SECONDS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8103be550cfedb474d99b10591af8c1697207e79eb3622f34e65edbefdf69756 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 2592000
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "DEFAULT_TERMINAL_STATE_RETENTION_SECONDS",
  "unit": "export"
}
```

</details>
