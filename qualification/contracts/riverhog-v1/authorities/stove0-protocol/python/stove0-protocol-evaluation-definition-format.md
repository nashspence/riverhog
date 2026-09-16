# stove0_protocol.EVALUATION_DEFINITION_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluation-definition-format:6cb87e869f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b6b9ada12"></a>
- <a id="s-d7aba34be9"></a>`distribution`: `stove0-protocol`
- <a id="s-e980315713"></a>`module`: `stove0_protocol`
- <a id="s-2ad5b81b02"></a>`name`: `EVALUATION_DEFINITION_FORMAT`
- <a id="s-226656039b"></a>`unit`: `export`

### Declared structure

- <a id="s-f09bf3e6ca"></a>`kind`: `"constant"`
- <a id="s-626720ba61"></a>`value`: `"stove0-evaluation-definition/v1"`

## Governing policies

- <a id="pa-698575dbb3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EVALUATION_DEFINITION_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85651a67aab0df6faa17805d6b146889c5fb324e6b36b734df664ab6c3cd6278 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-evaluation-definition/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EVALUATION_DEFINITION_FORMAT",
  "unit": "export"
}
```

</details>
