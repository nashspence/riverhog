# stove0_operator_contracts.WorkPage.from_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workpage-from-page:64940601b3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d9e1e074e"></a>
- <a id="s-55f4718e18"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e1d2f36033"></a>`module`: `stove0_operator_contracts`
- <a id="s-3af489c62a"></a>`name`: `from_page`
- <a id="s-a64b16f680"></a>`owner`: `stove0_operator_contracts.WorkPage`
- <a id="s-f145d725c0"></a>`unit`: `member`

### Declared structure

- <a id="s-81b6767adf"></a>`kind`: `"classmethod"`
- <a id="s-a9eaa64e61"></a>`signature`: `"\"(cls, page: 'Mapping[str, Any]') -> 'WorkPage'\""`

## Maintained corroboration

### Related interface records

- [WorkPage](stove0-operator-contracts-workpage.md)

## Governing policies

- <a id="pa-b9f5199fbc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkPage.from_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b9a63f25aafd77e90126303a7efe435fc65f0730161997f36f75008d7f243b3 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, page: 'Mapping[str, Any]') -> 'WorkPage'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "from_page",
  "owner": "stove0_operator_contracts.WorkPage",
  "unit": "member"
}
```

</details>
