# stove0_operator_contracts.BranchSetAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-branchsetadmittedevent:ebe45d3f32 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-643e4d67ba"></a>
| Field | Shape |
|---|---|
| <a id="s-162038edf0"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-95baea4ea7"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-0aacbe8bfe"></a>`module` | "stove0_operator_contracts" |
| <a id="s-9be6a4d574"></a>`name` | "BranchSetAdmittedEvent" |
| <a id="s-956d10453f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-bab00bf4cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.BranchSetAdmittedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c2f0e63728c5761d209daa3061de70e84c04aaf24fda93dddfdfa790632c66ec -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e9f15dd111d9ab1a6351617077e89f91ae78aa273dc4f798930cd8ef66763cef",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.branch-set.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.BranchSetAdmittedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "BranchSetAdmittedEvent",
  "unit": "export"
}
```
