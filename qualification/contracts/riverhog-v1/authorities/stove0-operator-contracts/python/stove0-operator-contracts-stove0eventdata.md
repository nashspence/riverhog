# stove0_operator_contracts.Stove0EventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventdata:c726d68af6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-90c034311c"></a>
| Field | Shape |
|---|---|
| <a id="s-381d8996d0"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f1d84cf39f"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-1754ba3e32"></a>`module` | "stove0_operator_contracts" |
| <a id="s-f11d8380b0"></a>`name` | "Stove0EventData" |
| <a id="s-e7650946bc"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.Stove0EventData.get](stove0-operator-contracts-stove0eventdata-get.md)
- [stove0_operator_contracts.Stove0EventData.__getitem__](stove0-operator-contracts-stove0eventdata-getitem.md)

## Governing policies

- <a id="pa-db17f8710a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4709830cc9256221c16bfcd93a69112e519994b6e974d289c6ba1957cafd2f95 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "158157692a2d44d063a5dc475153416e4e4b4ab76f497b963fb959bdf83e9d03",
    "signature": "'() -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0EventData",
  "unit": "export"
}
```
