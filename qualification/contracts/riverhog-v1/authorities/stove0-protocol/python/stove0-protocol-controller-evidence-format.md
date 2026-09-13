# stove0_protocol.CONTROLLER_EVIDENCE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-controller-evidence-format:2ad986584c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2110e896e2"></a>
| Field | Shape |
|---|---|
| <a id="s-8965b03651"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-c6d3de1fcd"></a>`distribution` | "stove0-protocol" |
| <a id="s-19edb42a53"></a>`module` | "stove0_protocol" |
| <a id="s-b2f17697b7"></a>`name` | "CONTROLLER_EVIDENCE_FORMAT" |
| <a id="s-50dc68ed8b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b368ae14a2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CONTROLLER_EVIDENCE_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0763981b4beba0c064ba3add2a00cef9f0d037d6a68458feb888aded37e7fa6 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-controller-evidence/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CONTROLLER_EVIDENCE_FORMAT",
  "unit": "export"
}
```
