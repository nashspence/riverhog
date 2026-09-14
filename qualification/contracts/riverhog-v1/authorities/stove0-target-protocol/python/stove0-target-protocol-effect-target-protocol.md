# stove0_target_protocol.EFFECT_TARGET_PROTOCOL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-effect-target-protocol:45afa12980 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f1fb6cec8"></a>
- <a id="s-dedac619ad"></a>`distribution`: `stove0-target-protocol`
- <a id="s-6c48390149"></a>`module`: `stove0_target_protocol`
- <a id="s-6d012ed7ee"></a>`name`: `EFFECT_TARGET_PROTOCOL`
- <a id="s-16b76ae3e3"></a>`unit`: `export`

### Declared structure

- <a id="s-617caf28f9"></a>`kind`: `"constant"`
- <a id="s-5f84590ed2"></a>`value`: `"stove0-effect-target/v1"`

## Governing policies

- <a id="pa-3730d20e38"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.EFFECT_TARGET_PROTOCOL`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75839f1846ea36b75b3c3850b71bde8fbd772cfa1baec6e9845bdeaa7c96f9bb -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-effect-target/v1"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "EFFECT_TARGET_PROTOCOL",
  "unit": "export"
}
```
