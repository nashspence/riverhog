# stove0_operator_contracts.AdmissionPhase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionphase:c582be9bca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41ef3c68d5"></a>
| Field | Shape |
|---|---|
| <a id="s-63e54e54f2"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-f1f38fbb18"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-c9d8667818"></a>`module` | "stove0_operator_contracts" |
| <a id="s-20ce191ac0"></a>`name` | "AdmissionPhase" |
| <a id="s-07019673f3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d07d4b8411"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPhase`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7dee40550f8f0e21a0919e5d7a1d519988dec5a98ef34d424592b30fbb1ddf01 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPhase",
  "unit": "export"
}
```
