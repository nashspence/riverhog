# stove0_operator_contracts.DepartureCatalog.canonical_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurecatalo-b0ad17861d:f88fa3bb09 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d949fd84f"></a>
- <a id="s-69e434df2a"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-0261fb1b4c"></a>`module`: `stove0_operator_contracts`
- <a id="s-a84f43622b"></a>`name`: `canonical_policies`
- <a id="s-1592fe5d2f"></a>`owner`: `stove0_operator_contracts.DepartureCatalog`
- <a id="s-30053418cc"></a>`unit`: `member`

### Declared structure

- <a id="s-fa63729740"></a>`kind`: `"classmethod"`
- <a id="s-d903ff414a"></a>`signature`: `"\"(cls, value: 'tuple[DeparturePolicy, ...]') -> 'tuple[DeparturePolicy, ...]'\""`

## Maintained corroboration

### Related interface records

- [DepartureCatalog](stove0-operator-contracts-departurecatalog.md)

## Governing policies

- <a id="pa-84085a9b1f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DepartureCatalog.canonical_policies`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d176eca7d6070c8a80af0aa3bfa29399482f94120a70d3f4fdaf7277f499865a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[DeparturePolicy, ...]') -> 'tuple[DeparturePolicy, ...]'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "canonical_policies",
  "owner": "stove0_operator_contracts.DepartureCatalog",
  "unit": "member"
}
```

</details>
