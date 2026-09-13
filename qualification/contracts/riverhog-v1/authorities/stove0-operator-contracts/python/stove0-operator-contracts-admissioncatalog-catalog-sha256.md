# stove0_operator_contracts.AdmissionCatalog.catalog_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissioncatalo-fe58a3d9e5:5a67163791 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d327be57e0"></a>
| Field | Shape |
|---|---|
| <a id="s-00d637fe3c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3059f358e2"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-57782e83d0"></a>`module` | "stove0_operator_contracts" |
| <a id="s-ac90b30f11"></a>`name` | "catalog_sha256" |
| <a id="s-f328623c9b"></a>`owner` | "stove0_operator_contracts.AdmissionCatalog" |
| <a id="s-c7ca1eb590"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionCatalog](stove0-operator-contracts-admissioncatalog.md)

## Governing policies

- <a id="pa-b9f50f7534"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionCatalog.catalog_sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db8d492347d63b0977834215a6547efdbff544a658c2f98526f5642f3efab21e -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "catalog_sha256",
  "owner": "stove0_operator_contracts.AdmissionCatalog",
  "unit": "member"
}
```
