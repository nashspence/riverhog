# stove0_target_protocol.TARGET_INPUT_PAGE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-target-input-page-max:71a89dad83 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9dd977d07"></a>
- <a id="s-1b03aaedbb"></a>`distribution`: `stove0-target-protocol`
- <a id="s-48a6535456"></a>`module`: `stove0_target_protocol`
- <a id="s-01a9a764fe"></a>`name`: `TARGET_INPUT_PAGE_MAX`
- <a id="s-415887f355"></a>`unit`: `export`

### Declared structure

- <a id="s-4ef95cb92e"></a>`kind`: `"constant"`
- <a id="s-7731064cca"></a>`value`: `256`

## Governing policies

- <a id="pa-35f42cd073"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TARGET_INPUT_PAGE_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acf886b4f032d1d7611bb350f6969e1c13361114c6ef022864f63ca1bee2e250 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 256
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TARGET_INPUT_PAGE_MAX",
  "unit": "export"
}
```

</details>
