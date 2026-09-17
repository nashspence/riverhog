# http_api_contracts.HttpOperationErrorAuthority.operation_ids

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationerrorauth-e7c27216b7:f94970dafa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b8de2dc5ab"></a>
- <a id="s-95631cc5bd"></a>`distribution`: `http-api-contracts`
- <a id="s-bd8b4fcc7e"></a>`module`: `http_api_contracts`
- <a id="s-4d52adea4f"></a>`name`: `operation_ids`
- <a id="s-0ffaea30a8"></a>`owner`: `http_api_contracts.HttpOperationErrorAuthority`
- <a id="s-266de9137f"></a>`unit`: `member`

### Declared structure

- <a id="s-3b98b78fd8"></a>`kind`: `"property"`
- <a id="s-940f83b69c"></a>`signature`: `"\"(self) -> 'frozenset[str]'\""`

## Maintained corroboration

### Related interface records

- [HttpOperationErrorAuthority](http-api-contracts-httpoperationerrorauthority.md)

## Governing policies

- <a id="pa-457d8f5f95"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationErrorAuthority.operation_ids`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9937ecb6afd6ade322bb51d44a156aff058d3ab2ed22bf6a9ffa13a4eccbe1a -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'frozenset[str]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "operation_ids",
  "owner": "http_api_contracts.HttpOperationErrorAuthority",
  "unit": "member"
}
```

</details>
