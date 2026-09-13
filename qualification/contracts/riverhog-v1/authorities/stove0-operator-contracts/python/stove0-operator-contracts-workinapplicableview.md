# stove0_operator_contracts.WorkInapplicableView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workinapplicableview:61ad7fcd62 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4459cf90ae"></a>
| Field | Shape |
|---|---|
| <a id="s-89f1d8e816"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5ca4613920"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-7fdfdcd881"></a>`module` | "stove0_operator_contracts" |
| <a id="s-ed2e15bd58"></a>`name` | "WorkInapplicableView" |
| <a id="s-8abf7d6945"></a>`unit` | "export" |

## Governing policies

- <a id="pa-5f1f4765b3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkInapplicableView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3502f345e1d8f46b73dfd9cfb67b3191537fc7a5c8db7f823f29ea92017edda -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6656e1447c29d937de4ad76ebaf92f3aee0431f6a1303726a004083da0376789",
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkInapplicableView",
  "unit": "export"
}
```
