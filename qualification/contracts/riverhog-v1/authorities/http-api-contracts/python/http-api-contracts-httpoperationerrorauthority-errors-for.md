# http_api_contracts.HttpOperationErrorAuthority.errors_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationerrorauth-143de95856:49256492a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55da008d7a"></a>
| Field | Shape |
|---|---|
| <a id="s-7a2557f154"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ce1506a96e"></a>`distribution` | "http-api-contracts" |
| <a id="s-0f8a54bd90"></a>`module` | "http_api_contracts" |
| <a id="s-eb76eeee14"></a>`name` | "errors_for" |
| <a id="s-6540193dca"></a>`owner` | "http_api_contracts.HttpOperationErrorAuthority" |
| <a id="s-6780f79584"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [http_api_contracts.HttpOperationErrorAuthority](http-api-contracts-httpoperationerrorauthority.md)

## Governing policies

- <a id="pa-78f68c817e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationErrorAuthority.errors_for`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f0d052b3ec29c6c2f5911f3e933a09ffca7aedb8b7518bc181397a7b62feccf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str') -> 'tuple[HttpErrorContract, ...]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "errors_for",
  "owner": "http_api_contracts.HttpOperationErrorAuthority",
  "unit": "member"
}
```
