# http_api_contracts.HttpResponseHeaderContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpresponseheadercontract:877cf54ae9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d12552e0d"></a>
- <a id="s-4fc4f751e6"></a>`distribution`: `http-api-contracts`
- <a id="s-3a9963b5d0"></a>`module`: `http_api_contracts`
- <a id="s-45e48ba31c"></a>`name`: `HttpResponseHeaderContract`
- <a id="s-3fc13150f2"></a>`unit`: `export`

### Declared structure

- <a id="s-61d83be94c"></a>`kind`: `"class"`
- <a id="s-d56244036b"></a>`signature`: `"\"(name: 'str', value_type: 'object' = <class 'str'>, description: 'str \| None' = None) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-e7b1478118"></a>`name` | `'str'` | `required` |
| <a id="s-60d207ce13"></a>`value_type` | `'object'` | `<class 'str'>` |
| <a id="s-dadc8af163"></a>`description` | `'str \| None'` | `None` |

## Governing policies

- <a id="pa-505737d030"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpResponseHeaderContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f6acb1482930326c7e5376f2a9d0ab500f19393e43a9b293b43769dec6312b5 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "name",
        "type": "'str'"
      },
      {
        "default": "<class 'str'>",
        "name": "value_type",
        "type": "'object'"
      },
      {
        "default": "None",
        "name": "description",
        "type": "'str | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(name: 'str', value_type: 'object' = <class 'str'>, description: 'str | None' = None) -> None\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HttpResponseHeaderContract",
  "unit": "export"
}
```

</details>
