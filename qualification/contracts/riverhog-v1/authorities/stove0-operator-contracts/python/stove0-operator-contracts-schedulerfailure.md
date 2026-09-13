# stove0_operator_contracts.SchedulerFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerfailure:df978e1f52 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1e38100b6"></a>
| Field | Shape |
|---|---|
| <a id="s-250bee2c0f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c97abb90e8"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-e121b2f226"></a>`module` | "stove0_operator_contracts" |
| <a id="s-54c7b5cb13"></a>`name` | "SchedulerFailure" |
| <a id="s-7f90a112c2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.SchedulerFailure.one_subject](stove0-operator-contracts-schedulerfailure-one-subject.md)

## Governing policies

- <a id="pa-122eda134a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2be0086918921366999cb8b4878f0a49f7b852e0794126e0ca21b1baf54acd6f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a833286ffda928b36d0a305881d883157303922614a8c4e55d7c3ff0134b32e2",
    "signature": "\"(*, event_id: str | None = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, error: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerFailure",
  "unit": "export"
}
```
