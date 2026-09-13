# stove0_operator_contracts.WorkClaimView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workclaimview:b7ed8b9383 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-05e377cda1"></a>
| Field | Shape |
|---|---|
| <a id="s-d60ab43ea6"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f2abc50185"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-bcd9d42f07"></a>`module` | "stove0_operator_contracts" |
| <a id="s-eacfc19fc2"></a>`name` | "WorkClaimView" |
| <a id="s-b21cf564b6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-968ca8b933"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkClaimView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe08e8abca852afb18c46f2aeb8751caba8cef1d91fc98e74e834b4b2cd7e294 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5836e31fafa2f07e9c5cb14ccd4562b4c1e719d136d58b41e3f193e6c594bc53",
    "signature": "'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkClaimView",
  "unit": "export"
}
```
