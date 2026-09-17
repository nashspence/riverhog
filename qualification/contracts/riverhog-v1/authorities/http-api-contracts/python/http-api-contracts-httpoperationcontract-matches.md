# http_api_contracts.HttpOperationContract.matches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationcontract-matches:b885afcce6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb542d1314"></a>
- <a id="s-f89d045dfc"></a>`distribution`: `http-api-contracts`
- <a id="s-9834eff961"></a>`module`: `http_api_contracts`
- <a id="s-72f1146f7a"></a>`name`: `matches`
- <a id="s-95896c4fa1"></a>`owner`: `http_api_contracts.HttpOperationContract`
- <a id="s-b6fadc28e3"></a>`unit`: `member`

### Declared structure

- <a id="s-54daa08f90"></a>`kind`: `"method"`
- <a id="s-a4f827e3e5"></a>`signature`: `"\"(self, method: 'str', path: 'str') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [HttpOperationContract](http-api-contracts-httpoperationcontract.md)

## Governing policies

- <a id="pa-937d24c2d0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationContract.matches`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04ed27a1569fbcc31b38c2a5816d5a7bfe02e4dbf0b8d03ece94c852f6330004 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str') -> 'bool'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "matches",
  "owner": "http_api_contracts.HttpOperationContract",
  "unit": "member"
}
```

</details>
