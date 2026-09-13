# stove0_operator_contracts.AdmissionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpage:c08d98b36c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-996bf7deab"></a>
| Field | Shape |
|---|---|
| <a id="s-80a6f8ec47"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c04bead7de"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-861ee0bcef"></a>`module` | "stove0_operator_contracts" |
| <a id="s-7cb454134d"></a>`name` | "AdmissionPage" |
| <a id="s-561a6b0083"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3d604dd7a3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1c17444a70a4d19150bc984307f0d84a205754c18ca1a9af8ac9d1c12c79b70 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1c999fdfd073ab6da2b05a6189166ce2f7dbff29a64c6e8e32695cff71e09607",
    "signature": "\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['created_at', 'updated_at', 'state', 'admission_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], policy_id: str | None, state: Optional[Literal['intent', 'previewed', 'work_bound']], admissions: tuple[stove0_operator_contracts.AdmissionView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPage",
  "unit": "export"
}
```
