# stove0_target_protocol.EFFECT_RECEIPT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-effect-receipt-format:cf562a03b6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb469fa582"></a>
- <a id="s-aad472e5c7"></a>`distribution`: `stove0-target-protocol`
- <a id="s-19780ca25a"></a>`module`: `stove0_target_protocol`
- <a id="s-1e779bc9a7"></a>`name`: `EFFECT_RECEIPT_FORMAT`
- <a id="s-925d76f73d"></a>`unit`: `export`

### Declared structure

- <a id="s-de72ca6c4a"></a>`kind`: `"constant"`
- <a id="s-b79657af80"></a>`value`: `"stove0-external-effect-receipt/v1"`

## Governing policies

- <a id="pa-1b0cdecd62"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.EFFECT_RECEIPT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b43f8fb7d08caef8be30116442ec590b00aefdb0898215bb2d969463edbac68 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-external-effect-receipt/v1"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "EFFECT_RECEIPT_FORMAT",
  "unit": "export"
}
```

</details>
