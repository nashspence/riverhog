# stove0_protocol.BRANCH_SET_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branch-set-format:b73038b372 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-013d88df3a"></a>
| Field | Shape |
|---|---|
| <a id="s-9b561595c8"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-29635c8773"></a>`distribution` | "stove0-protocol" |
| <a id="s-324b88bf59"></a>`module` | "stove0_protocol" |
| <a id="s-7fac552880"></a>`name` | "BRANCH_SET_FORMAT" |
| <a id="s-a77962877c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b84eef5d43"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BRANCH_SET_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fdbf5c60aec08b63670a32614f1d9287d0e6593c11dd029b066808d42327ffa1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-branch-set/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BRANCH_SET_FORMAT",
  "unit": "export"
}
```
