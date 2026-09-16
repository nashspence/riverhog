# http_api_contracts.HttpOperationErrorAuthority.accepts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationerrorauth-ec17c97e6f:af5ef855e9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab54d0cc5c"></a>
- <a id="s-cf537230ec"></a>`distribution`: `http-api-contracts`
- <a id="s-62b2773af1"></a>`module`: `http_api_contracts`
- <a id="s-792936161b"></a>`name`: `accepts`
- <a id="s-267e4338f2"></a>`owner`: `http_api_contracts.HttpOperationErrorAuthority`
- <a id="s-5e312255aa"></a>`unit`: `member`

### Declared structure

- <a id="s-40897c8fd1"></a>`kind`: `"method"`
- <a id="s-598ca331b6"></a>`signature`: `"\"(self, operation_id: 'str', *, status: 'int', code: 'str') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [HttpOperationErrorAuthority](http-api-contracts-httpoperationerrorauthority.md)

## Governing policies

- <a id="pa-1b54ccfabf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationErrorAuthority.accepts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a18832d7dde32cdee3c5f17098a0f170cf35c7f99ed877dbc104304c888b5134 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str', *, status: 'int', code: 'str') -> 'bool'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "accepts",
  "owner": "http_api_contracts.HttpOperationErrorAuthority",
  "unit": "member"
}
```

</details>
