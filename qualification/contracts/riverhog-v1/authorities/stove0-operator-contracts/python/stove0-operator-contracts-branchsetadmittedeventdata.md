# stove0_operator_contracts.BranchSetAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-branchsetadmittedeventdata:f768fa48e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-811f53197c"></a>
| Field | Shape |
|---|---|
| <a id="s-f74929d33b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-88d0a14002"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-f3cac77eb9"></a>`module` | "stove0_operator_contracts" |
| <a id="s-7a4dad50a7"></a>`name` | "BranchSetAdmittedEventData" |
| <a id="s-7066bfad7e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-72f39e283f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.BranchSetAdmittedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c59dfb2d1a5cf8d315165e18fa0e084d03ee85eb7eddde00b2043f66085b777 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8915aca1dade95650ccfb03c5ac47cf18fb3cd693a2c2becad387ba7624688f6",
    "signature": "\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_count: Annotated[int, Ge(ge=1)], admitted_work_count: Annotated[int, Ge(ge=1)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "BranchSetAdmittedEventData",
  "unit": "export"
}
```
