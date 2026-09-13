# stove0_operator_contracts.WorkCreateIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatein:346ce08e5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19eca1b23f"></a>
| Field | Shape |
|---|---|
| <a id="s-3849eb4cc5"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d020902aca"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-8b38ce800c"></a>`module` | "stove0_operator_contracts" |
| <a id="s-2ac58b77aa"></a>`name` | "WorkCreateIn" |
| <a id="s-7e7064a029"></a>`unit` | "export" |

## Governing policies

- <a id="pa-566c0baa8d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreateIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 760fff8b93eb7882fab04087221787abcd04cb2e5b855510191ac3c7f6a81d76 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ea611476780c1760169ddacd75b984a7c9b035cf96394d12250b835495e6a748",
    "signature": "\"(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreateIn",
  "unit": "export"
}
```
