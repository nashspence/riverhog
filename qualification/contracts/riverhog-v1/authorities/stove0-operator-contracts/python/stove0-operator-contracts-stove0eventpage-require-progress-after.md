# stove0_operator_contracts.Stove0EventPage.require_progress_after

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventpage-5265aa9d11:0ae515a124 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-504b405fe8"></a>
| Field | Shape |
|---|---|
| <a id="s-bac928b686"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1e85885bcf"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-d5e8a30931"></a>`module` | "stove0_operator_contracts" |
| <a id="s-b78ec81a35"></a>`name` | "require_progress_after" |
| <a id="s-e8f5175d3b"></a>`owner` | "stove0_operator_contracts.Stove0EventPage" |
| <a id="s-cafbcca230"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.Stove0EventPage](stove0-operator-contracts-stove0eventpage.md)

## Governing policies

- <a id="pa-9a5963c0ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventPage.require_progress_after`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc7998722e5fb6ec63c3123aa26587a67c2cc586bc7feec70b10ea8ff110c5eb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str') -> 'None'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "require_progress_after",
  "owner": "stove0_operator_contracts.Stove0EventPage",
  "unit": "member"
}
```
