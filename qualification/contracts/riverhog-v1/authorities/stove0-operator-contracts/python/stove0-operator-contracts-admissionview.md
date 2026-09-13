# stove0_operator_contracts.AdmissionView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionview:33b0586c5a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c22c4d3313"></a>
| Field | Shape |
|---|---|
| <a id="s-bf3daf2abb"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-584c6f5337"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-b38e807287"></a>`module` | "stove0_operator_contracts" |
| <a id="s-498b858bfe"></a>`name` | "AdmissionView" |
| <a id="s-94c1485c5c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionView.exact_stage](stove0-operator-contracts-admissionview-exact-stage.md)

## Governing policies

- <a id="pa-1b2565cc2e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d0132477af55ff7606d5b927940fdaabbeec1d912d4d3261c5580a7dd585b65 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "242e77047ed695bde61d11c1d145155b17b1c639bbfac0c8ad71f70f34d53339",
    "signature": "\"(*, intent: stove0_operator_contracts.AdmissionIntent, state: Literal['intent', 'previewed', 'work_bound'], preview_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, attempt_count: Annotated[int, Ge(ge=0)], next_attempt_at: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None, failure: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionView",
  "unit": "export"
}
```
