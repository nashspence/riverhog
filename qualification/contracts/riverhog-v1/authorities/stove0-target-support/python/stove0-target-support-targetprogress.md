# stove0_target_support.TargetProgress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetprogress:975516b022 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a51cad10c3"></a>
| Field | Shape |
|---|---|
| <a id="s-df3bbf0587"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-96f9e89a7b"></a>`distribution` | "stove0-target-support" |
| <a id="s-b08cb82556"></a>`module` | "stove0_target_support" |
| <a id="s-9a5a7a434e"></a>`name` | "TargetProgress" |
| <a id="s-58b9269cf2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetProgress.validate_total](stove0-target-support-targetprogress-validate-total.md)

## Governing policies

- <a id="pa-0504dd7528"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetProgress`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ace2eb3b3a2bf7e6608b70df4fee70255efd026fdd41d7ed0e44e3616fbfb918 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3b531b19161c4e085385a0c27312bc32072b1dda15095512891696fd64be3fca",
    "signature": "'(*, phase: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], completed: Annotated[int, Ge(ge=0)], total: Annotated[int | None, Ge(ge=0)] = None, unit: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None) -> None'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetProgress",
  "unit": "export"
}
```
