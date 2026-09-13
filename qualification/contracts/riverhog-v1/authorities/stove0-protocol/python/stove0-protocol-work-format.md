# stove0_protocol.WORK_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-work-format:e39e304f9a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b21dec730a"></a>
| Field | Shape |
|---|---|
| <a id="s-1dc41e4180"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-d427a6cf0a"></a>`distribution` | "stove0-protocol" |
| <a id="s-13ea27230e"></a>`module` | "stove0_protocol" |
| <a id="s-fd7e3b3ddc"></a>`name` | "WORK_FORMAT" |
| <a id="s-c6813d7313"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1b871f1b5c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WORK_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5df6458cb33713abf232fc058e6fa458a8cc3d2c6eeadbcf214e0703330398a6 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-work/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WORK_FORMAT",
  "unit": "export"
}
```
