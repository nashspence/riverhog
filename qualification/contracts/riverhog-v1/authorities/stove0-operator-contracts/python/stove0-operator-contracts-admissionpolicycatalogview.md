# stove0_operator_contracts.AdmissionPolicyCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicycatalogview:02bee760ab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61e469f9ee"></a>
| Field | Shape |
|---|---|
| <a id="s-0f4feffb91"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8675565fed"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-075b595da8"></a>`module` | "stove0_operator_contracts" |
| <a id="s-d246c344a0"></a>`name` | "AdmissionPolicyCatalogView" |
| <a id="s-46a485b2e3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4deee81d84"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicyCatalogView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4342d33bc513fbba6cfdf1512053327a768c934a1f6bcea4a9355f50b0242bf -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ec3d8568a5b10756746eae442f63e658e3f9aff83a690be9759d4d7ef30748d3",
    "signature": "\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policies: tuple[stove0_operator_contracts.AdmissionPolicyStatus, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPolicyCatalogView",
  "unit": "export"
}
```
