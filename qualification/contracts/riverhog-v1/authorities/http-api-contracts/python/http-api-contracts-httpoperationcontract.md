# http_api_contracts.HttpOperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationcontract:43a5de12e5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4697ee66fd"></a>
- <a id="s-3b6f6c83a4"></a>`distribution`: `http-api-contracts`
- <a id="s-1f3a479549"></a>`module`: `http_api_contracts`
- <a id="s-b780ffae35"></a>`name`: `HttpOperationContract`
- <a id="s-78eda8b34e"></a>`unit`: `export`

### Declared structure

- <a id="s-f6586f5a41"></a>`kind`: `"class"`
- <a id="s-8779856aba"></a>`signature`: `"'(method: \"Literal[\\'GET\\', \\'POST\\', \\'PUT\\', \\'DELETE\\', \\'PATCH\\']\", path: \\'str\\', request_type: \\'object \| None\\' = None, response_type: \\'object \| None\\' = None, request_kind: \\'HttpBodyKind\\' = \\'none\\', response_kind: \\'HttpBodyKind\\' = \\'json\\', success_statuses: \\'tuple[int, ...]\\' = (200,), errors: \\'tuple[HttpErrorContract, ...]\\' = (), path_parameters: \\'tuple[HttpPathParameterContract, ...]\\' = (), response_headers: \\'tuple[HttpResponseHeaderContract, ...]\\' = (), error_type: \\'type[BaseModel]\\' = <class \\'http_api_contracts.ErrorResponse\\'>) -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-09f1413f71"></a>`method` | `"Literal['GET', 'POST', 'PUT', 'DELETE', 'PATCH']"` | `required` |
| <a id="s-10bffcbb2e"></a>`path` | `'str'` | `required` |
| <a id="s-b411f91fe8"></a>`request_type` | `'object \| None'` | `None` |
| <a id="s-fa1f370fe1"></a>`response_type` | `'object \| None'` | `None` |
| <a id="s-d662577c83"></a>`request_kind` | `'HttpBodyKind'` | `'none'` |
| <a id="s-aa4ca0e78e"></a>`response_kind` | `'HttpBodyKind'` | `'json'` |
| <a id="s-815928fe99"></a>`success_statuses` | `'tuple[int, ...]'` | `(200,)` |
| <a id="s-846a9bb9ab"></a>`errors` | `'tuple[HttpErrorContract, ...]'` | `()` |
| <a id="s-3b92073354"></a>`path_parameters` | `'tuple[HttpPathParameterContract, ...]'` | `()` |
| <a id="s-e63129c26b"></a>`response_headers` | `'tuple[HttpResponseHeaderContract, ...]'` | `()` |
| <a id="s-45f1ec21d4"></a>`error_type` | `'type[BaseModel]'` | `<class 'http_api_contracts.ErrorResponse'>` |

## Maintained corroboration

### Related interface records

- [accepts_error](http-api-contracts-httpoperationcontract-accepts-error.md)
- [error_statuses](http-api-contracts-httpoperationcontract-error-statuses.md)
- [matches](http-api-contracts-httpoperationcontract-matches.md)

## Governing policies

- <a id="pa-de6fdf542e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3123f489d4d2f618b0f8a05cab4c3525454360f14b319b9b84aa8c2c736395f -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "method",
        "type": "\"Literal['GET', 'POST', 'PUT', 'DELETE', 'PATCH']\""
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "request_type",
        "type": "'object | None'"
      },
      {
        "default": "None",
        "name": "response_type",
        "type": "'object | None'"
      },
      {
        "default": "'none'",
        "name": "request_kind",
        "type": "'HttpBodyKind'"
      },
      {
        "default": "'json'",
        "name": "response_kind",
        "type": "'HttpBodyKind'"
      },
      {
        "default": "(200,)",
        "name": "success_statuses",
        "type": "'tuple[int, ...]'"
      },
      {
        "default": "()",
        "name": "errors",
        "type": "'tuple[HttpErrorContract, ...]'"
      },
      {
        "default": "()",
        "name": "path_parameters",
        "type": "'tuple[HttpPathParameterContract, ...]'"
      },
      {
        "default": "()",
        "name": "response_headers",
        "type": "'tuple[HttpResponseHeaderContract, ...]'"
      },
      {
        "default": "<class 'http_api_contracts.ErrorResponse'>",
        "name": "error_type",
        "type": "'type[BaseModel]'"
      }
    ],
    "kind": "class",
    "signature": "'(method: \"Literal[\\'GET\\', \\'POST\\', \\'PUT\\', \\'DELETE\\', \\'PATCH\\']\", path: \\'str\\', request_type: \\'object | None\\' = None, response_type: \\'object | None\\' = None, request_kind: \\'HttpBodyKind\\' = \\'none\\', response_kind: \\'HttpBodyKind\\' = \\'json\\', success_statuses: \\'tuple[int, ...]\\' = (200,), errors: \\'tuple[HttpErrorContract, ...]\\' = (), path_parameters: \\'tuple[HttpPathParameterContract, ...]\\' = (), response_headers: \\'tuple[HttpResponseHeaderContract, ...]\\' = (), error_type: \\'type[BaseModel]\\' = <class \\'http_api_contracts.ErrorResponse\\'>) -> None'"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HttpOperationContract",
  "unit": "export"
}
```

</details>
