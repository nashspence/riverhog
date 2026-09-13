# stove0_operator_contracts.EvaluationPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationpage:c0d9fc40bc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d6ed7b84c"></a>
| Field | Shape |
|---|---|
| <a id="s-ba9321ef98"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4ba1e63d2d"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-a22fde2f64"></a>`module` | "stove0_operator_contracts" |
| <a id="s-03ded66168"></a>`name` | "EvaluationPage" |
| <a id="s-84c3be3ae2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationPage.from_page](stove0-operator-contracts-evaluationpage-from-page.md)

## Governing policies

- <a id="pa-488caef597"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3293c88bd782e57bd665bccaa239460ffa9f4b11086c407dc55819ac922d8cc -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "00a56a81565ce9921f29db6856ca944ed6f8167189d80c36c596df55ec5c0950",
    "signature": "\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['updated_at', 'phase', 'evaluation_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], evaluations: tuple[stove0_operator_contracts.EvaluationView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationPage",
  "unit": "export"
}
```
