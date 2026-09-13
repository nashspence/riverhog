# stove0_operator_contracts.WorkFailureView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workfailureview:d2b84e4019 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d68b33e221"></a>
| Field | Shape |
|---|---|
| <a id="s-89d8ed55cc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-88015493c5"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-03d01889e2"></a>`module` | "stove0_operator_contracts" |
| <a id="s-03b57a20e3"></a>`name` | "WorkFailureView" |
| <a id="s-9225fcf532"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f1a759e61c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkFailureView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81af4105ca6d77668bca07abf13b2671d581fc860287374e2b099e953d7f5927 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3a61374424ae470a2c694ac13502faf98f91af05992638c02e1d5b5380dac0c4",
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkFailureView",
  "unit": "export"
}
```
