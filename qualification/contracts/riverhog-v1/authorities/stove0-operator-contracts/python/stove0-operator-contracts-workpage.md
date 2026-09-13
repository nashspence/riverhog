# stove0_operator_contracts.WorkPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workpage:33311faae3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2dd2b8da0b"></a>
| Field | Shape |
|---|---|
| <a id="s-90e05aecf2"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8b7fbfa500"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-6fd93a611c"></a>`module` | "stove0_operator_contracts" |
| <a id="s-b3e67ec67c"></a>`name` | "WorkPage" |
| <a id="s-447dd88c07"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.WorkPage.from_page](stove0-operator-contracts-workpage-from-page.md)

## Governing policies

- <a id="pa-3c28a21596"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9e7715d2ab9dca1c808c222ac98d4c4442798ddaf17da83e045a87fae688052 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d4dcd0f75a6790af3eececd4020f2a6b942bf376472f88997fc91584c8ce8095",
    "signature": "\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['updated_at', 'phase', 'work_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], work: tuple[stove0_operator_contracts.WorkView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkPage",
  "unit": "export"
}
```
