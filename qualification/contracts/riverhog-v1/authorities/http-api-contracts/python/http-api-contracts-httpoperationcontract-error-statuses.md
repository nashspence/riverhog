# http_api_contracts.HttpOperationContract.error_statuses

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationcontract-c34dcf947a:dae945e3a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-301c16594f"></a>
| Field | Shape |
|---|---|
| <a id="s-6f82272599"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b86d0f41b7"></a>`distribution` | "http-api-contracts" |
| <a id="s-25630ad114"></a>`module` | "http_api_contracts" |
| <a id="s-eae9dfd48c"></a>`name` | "error_statuses" |
| <a id="s-fdf24b8654"></a>`owner` | "http_api_contracts.HttpOperationContract" |
| <a id="s-744dbbf476"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [http_api_contracts.HttpOperationContract](http-api-contracts-httpoperationcontract.md)

## Governing policies

- <a id="pa-0252194400"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationContract.error_statuses`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23a4c4a6aa236dab9d66f147c1571a51b8d25b85de54affee6501598cc97ce8c -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'tuple[int, ...]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "error_statuses",
  "owner": "http_api_contracts.HttpOperationContract",
  "unit": "member"
}
```
