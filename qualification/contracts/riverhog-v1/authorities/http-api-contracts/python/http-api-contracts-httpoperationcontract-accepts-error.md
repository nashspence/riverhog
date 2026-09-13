# http_api_contracts.HttpOperationContract.accepts_error

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationcontract-2845fd67a7:bc1c8869d5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23e8aaf803"></a>
| Field | Shape |
|---|---|
| <a id="s-da2f0e0f0a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a3f5999d63"></a>`distribution` | "http-api-contracts" |
| <a id="s-180519ea26"></a>`module` | "http_api_contracts" |
| <a id="s-88258cdc10"></a>`name` | "accepts_error" |
| <a id="s-92e786511b"></a>`owner` | "http_api_contracts.HttpOperationContract" |
| <a id="s-58d871ed0b"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [http_api_contracts.HttpOperationContract](http-api-contracts-httpoperationcontract.md)

## Governing policies

- <a id="pa-b11e314ff5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationContract.accepts_error`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6fb7a7de351a33a7a432e8beab5d62700f16e290149ad45ae1da2c23433e9d10 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, status: 'int', code: 'str') -> 'bool'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "accepts_error",
  "owner": "http_api_contracts.HttpOperationContract",
  "unit": "member"
}
```
