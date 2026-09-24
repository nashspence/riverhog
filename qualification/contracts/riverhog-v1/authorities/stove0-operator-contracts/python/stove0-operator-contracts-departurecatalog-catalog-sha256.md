# stove0_operator_contracts.DepartureCatalog.catalog_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurecatalo-34f6b7acf2:fea5d05b68 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cfa848ab5b"></a>
- <a id="s-41bd965b92"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-aab8112737"></a>`module`: `stove0_operator_contracts`
- <a id="s-57e4512c69"></a>`name`: `catalog_sha256`
- <a id="s-7b0e0c21e0"></a>`owner`: `stove0_operator_contracts.DepartureCatalog`
- <a id="s-6a90b3451f"></a>`unit`: `member`

### Declared structure

- <a id="s-6b56dca8b9"></a>`kind`: `"property"`
- <a id="s-d18713a327"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [DepartureCatalog](stove0-operator-contracts-departurecatalog.md)

## Governing policies

- <a id="pa-05e13fa243"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DepartureCatalog.catalog_sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a4d03b413cffb1dd6e02f1b42d7623f9cf4dd9711e8e457ce0cd236efcef43d -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "catalog_sha256",
  "owner": "stove0_operator_contracts.DepartureCatalog",
  "unit": "member"
}
```

</details>
