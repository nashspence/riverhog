# stove0_operator_contracts.AdmissionCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissioncatalog:95e5135c66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6dcc5be0e9"></a>
| Field | Shape |
|---|---|
| <a id="s-35c980ed4e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3ab6af11dd"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-d3474dcc8d"></a>`module` | "stove0_operator_contracts" |
| <a id="s-dc103249f8"></a>`name` | "AdmissionCatalog" |
| <a id="s-b029c2dcf2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionCatalog.canonical_policies](stove0-operator-contracts-admissioncatalog-canonical-policies.md)
- [stove0_operator_contracts.AdmissionCatalog.catalog_sha256](stove0-operator-contracts-admissioncatalog-catalog-sha256.md)

## Governing policies

- <a id="pa-ece43c0ae8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionCatalog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88b48d4dff2d02392b7d9b8a37c58e90a635eea77680745f28826b77f0ed169f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "00ecac8a3c71b27fc2027d9df838959cdfe480be931fd7518ea0f785f84b01c0",
    "signature": "\"(*, format: Literal['stove0-admissions/v1'] = 'stove0-admissions/v1', policies: Annotated[tuple[stove0_operator_contracts.AdmissionPolicy, ...], MaxLen(max_length=100)] = ()) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionCatalog",
  "unit": "export"
}
```
