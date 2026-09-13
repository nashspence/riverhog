# stove0_operator_contracts.AdmissionRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionrun:15d9669c48 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb4a406e6b"></a>
| Field | Shape |
|---|---|
| <a id="s-f0e8417378"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2037711a36"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-55d2cf5a48"></a>`module` | "stove0_operator_contracts" |
| <a id="s-a2f0c5be9f"></a>`name` | "AdmissionRun" |
| <a id="s-67f208d163"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7679e6dae4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionRun`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd437068801f1f43fe108935f2c7177407927ef1000c7f6908a5712ea26213b4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6025bbb477e8e9a478cc47dd8e7d4a3d3ff478a918d32d17a35ba24e2a32a9e8",
    "signature": "'(*, progressed: tuple[str, ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...] = ()) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionRun",
  "unit": "export"
}
```
