# stove0_operator_contracts.JoinAdmittedEventData.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-joinadmittedeventdata-get:b702614ef1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9da02779d7"></a>
- <a id="s-feaf03f4b7"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f2d74382d7"></a>`module`: `stove0_operator_contracts`
- <a id="s-45af7eb04c"></a>`name`: `get`
- <a id="s-01cd04d77a"></a>`owner`: `stove0_operator_contracts.JoinAdmittedEventData`
- <a id="s-9af1a4fec4"></a>`unit`: `member`

### Declared structure

- <a id="s-144674d711"></a>`kind`: `"method"`
- <a id="s-492e92baf6"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [JoinAdmittedEventData](stove0-operator-contracts-joinadmittedeventdata.md)

## Governing policies

- <a id="pa-8205d059a4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JoinAdmittedEventData.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 708ebb3ff7004fb6e0bea273389a24cd8501ea44641457ad873da3a4e55eba1a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "get",
  "owner": "stove0_operator_contracts.JoinAdmittedEventData",
  "unit": "member"
}
```

</details>
