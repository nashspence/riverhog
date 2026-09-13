# stove0_target_protocol.update_input_disposition_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-update-input-dispo-659488f3ae:16b06c00da -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd61cec839"></a>
| Field | Shape |
|---|---|
| <a id="s-ac7fd67d75"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-46b06a2a4b"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-f2d68ad3c8"></a>`module` | "stove0_target_protocol" |
| <a id="s-a88d102c87"></a>`name` | "update_input_disposition_commitment" |
| <a id="s-c4b434c435"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c7166224cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.update_input_disposition_commitment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43034e2194902c9c20127c39d581320c07290c6dac4e7031e5244359da577470 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'Any', *, ordinal: 'int', disposition: 'InputDispositionDeclaration') -> 'None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "update_input_disposition_commitment",
  "unit": "export"
}
```
