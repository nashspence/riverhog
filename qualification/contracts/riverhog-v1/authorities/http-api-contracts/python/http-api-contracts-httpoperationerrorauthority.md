# http_api_contracts.HttpOperationErrorAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationerrorauthority:e5605c6c1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-01fb327fce"></a>
- <a id="s-c07e8f564a"></a>`distribution`: `http-api-contracts`
- <a id="s-e51584b0dd"></a>`module`: `http_api_contracts`
- <a id="s-d6d6712eed"></a>`name`: `HttpOperationErrorAuthority`
- <a id="s-7399688125"></a>`unit`: `export`

### Declared structure

- <a id="s-3d0b5c999e"></a>`kind`: `"class"`
- <a id="s-8b4595204e"></a>`signature`: `"\"(*, common: 'Collection[HttpErrorContract]', operation: 'Mapping[str, Collection[HttpErrorContract]]', exact: 'Mapping[str, Collection[HttpErrorContract]] \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [http_api_contracts.HttpOperationErrorAuthority.errors_for](http-api-contracts-httpoperationerrorauthority-errors-for.md)
- [http_api_contracts.HttpOperationErrorAuthority.from_codes](http-api-contracts-httpoperationerrorauthority-from-codes.md)
- [http_api_contracts.HttpOperationErrorAuthority.operation_ids](http-api-contracts-httpoperationerrorauthority-operation-ids.md)
- [http_api_contracts.HttpOperationErrorAuthority.accepts](http-api-contracts-httpoperationerrorauthority-accepts.md)

## Governing policies

- <a id="pa-d13e131544"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationErrorAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d32370d5bf947f63a4bb898797201b0af108035c6fef87ea4ca16c572ce1cd7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, common: 'Collection[HttpErrorContract]', operation: 'Mapping[str, Collection[HttpErrorContract]]', exact: 'Mapping[str, Collection[HttpErrorContract]] | None' = None) -> 'None'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HttpOperationErrorAuthority",
  "unit": "export"
}
```
