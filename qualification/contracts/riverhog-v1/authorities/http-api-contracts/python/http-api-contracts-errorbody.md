# http_api_contracts.ErrorBody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-errorbody:51f57b6cf2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-66f5e845ea"></a>
| Field | Shape |
|---|---|
| <a id="s-ddb2f2b858"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a02aeebd1a"></a>`distribution` | "http-api-contracts" |
| <a id="s-7bd38abf12"></a>`module` | "http_api_contracts" |
| <a id="s-6b37ed451d"></a>`name` | "ErrorBody" |
| <a id="s-1660f7149b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d1eddee41c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.ErrorBody`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84da6f5626b193476a70086361d40f28ab8acdbd423c4ff6b5fecb5b7bc0d623 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6760c34bd592b6639dc349282b6e02f6cebfa1985aa1a5b2c1943c0faa367741",
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1)], message: Annotated[str, MinLen(min_length=1)], details: dict[str, typing.Any] | None = None) -> None'"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "ErrorBody",
  "unit": "export"
}
```
