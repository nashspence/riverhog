# stove0_operator_contracts.JoinAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-joinadmittedeventdata:41a71f9255 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34bc53e1a5"></a>
| Field | Shape |
|---|---|
| <a id="s-b8af897b90"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7bc18ce55a"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-f91b57dfc8"></a>`module` | "stove0_operator_contracts" |
| <a id="s-8773b7f30d"></a>`name` | "JoinAdmittedEventData" |
| <a id="s-134471b925"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c1c198151d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JoinAdmittedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc780cd6e98ed0aa5909524ff1314f4a3c3b140c5313d2dca8687dd2240cbe6e -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "dab536a055b1029ef8bc9d5c83589286aaeab4d73dbf9d5fc9a1f6b951f16e8d",
    "signature": "\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "JoinAdmittedEventData",
  "unit": "export"
}
```
