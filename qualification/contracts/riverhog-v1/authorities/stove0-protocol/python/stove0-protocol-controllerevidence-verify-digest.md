# stove0_protocol.ControllerEvidence.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-controllerevidence-verify-digest:dc995416c7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-376966ffb1"></a>
| Field | Shape |
|---|---|
| <a id="s-ae2f8d4a8c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a9b0dda228"></a>`distribution` | "stove0-protocol" |
| <a id="s-8c77b4a443"></a>`module` | "stove0_protocol" |
| <a id="s-82255cf042"></a>`name` | "verify_digest" |
| <a id="s-a081c5a1d4"></a>`owner` | "stove0_protocol.ControllerEvidence" |
| <a id="s-a6f190038b"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ControllerEvidence](stove0-protocol-controllerevidence.md)

## Governing policies

- <a id="pa-3ea0eadc85"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ControllerEvidence.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf5d8533de0bf764da7e4792c2d19ef70111ede96c68d3cb5918eb154a019a61 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.ControllerEvidence",
  "unit": "member"
}
```
