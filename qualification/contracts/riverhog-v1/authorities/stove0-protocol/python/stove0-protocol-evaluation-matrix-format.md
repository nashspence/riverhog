# stove0_protocol.EVALUATION_MATRIX_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluation-matrix-format:cd1eaab5b8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-72d12fdfda"></a>
| Field | Shape |
|---|---|
| <a id="s-3e2e178c4b"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-6ddf69f76e"></a>`distribution` | "stove0-protocol" |
| <a id="s-848f990102"></a>`module` | "stove0_protocol" |
| <a id="s-721b7be0e1"></a>`name` | "EVALUATION_MATRIX_FORMAT" |
| <a id="s-5962fdbdb0"></a>`unit` | "export" |

## Governing policies

- <a id="pa-df539d6c65"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EVALUATION_MATRIX_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14cd3ebe965bac8e47c8cf0d83b07569985d550407c03cbb881911a679525cc0 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-evaluation-matrix/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EVALUATION_MATRIX_FORMAT",
  "unit": "export"
}
```
