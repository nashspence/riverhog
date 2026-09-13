# stove0_operator_contracts.Stove0EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventpage:953f89e83c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cf1c87427"></a>
| Field | Shape |
|---|---|
| <a id="s-060e83f6cc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fd58b575c8"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-f3a6e143ee"></a>`module` | "stove0_operator_contracts" |
| <a id="s-e8aa9fc6f7"></a>`name` | "Stove0EventPage" |
| <a id="s-5c6a732e20"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.Stove0EventPage.require_progress_after](stove0-operator-contracts-stove0eventpage-require-progress-after.md)

## Governing policies

- <a id="pa-2b9942b22c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c41e5992ca85d253a32d316ed2cffa49db3283041716d2b3fee0bb5f12e33a9f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7dfcf567d2d57418b2348d851a596eaa26f7542d74fbbf52e84a54984b0e33ad",
    "signature": "'(*, events: list[Stove0LifecycleEvent], next_cursor: str, has_more: bool) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0EventPage",
  "unit": "export"
}
```
