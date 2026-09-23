# stove0_operator_contracts.WorkView.exact_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workview-exact-identity:d8f4cc2f92 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f047eb9e1"></a>
- <a id="s-107634a6b8"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-487fd198af"></a>`module`: `stove0_operator_contracts`
- <a id="s-4f5584be50"></a>`name`: `exact_identity`
- <a id="s-8a541e5431"></a>`owner`: `stove0_operator_contracts.WorkView`
- <a id="s-e93da55bfc"></a>`unit`: `member`

### Declared structure

- <a id="s-0f89f6c4e1"></a>`kind`: `"method"`
- <a id="s-6ab14da8fd"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkView](stove0-operator-contracts-workview.md)

## Governing policies

- <a id="pa-5d71c0b66a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkView.exact_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6534d6e7fcbf504b2402490b0663a558c13d1d806f6fc08df2c5c432d58da6b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_identity",
  "owner": "stove0_operator_contracts.WorkView",
  "unit": "member"
}
```

</details>
