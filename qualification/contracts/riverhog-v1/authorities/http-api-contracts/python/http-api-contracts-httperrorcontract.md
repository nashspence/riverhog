# http_api_contracts.HttpErrorContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httperrorcontract:8e607bcede -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f620769d5"></a>
- <a id="s-e6ec0189b3"></a>`distribution`: `http-api-contracts`
- <a id="s-f16772f287"></a>`module`: `http_api_contracts`
- <a id="s-9aca6d0207"></a>`name`: `HttpErrorContract`
- <a id="s-a7684f9b74"></a>`unit`: `export`

### Declared structure

- <a id="s-b83d2050d4"></a>`kind`: `"class"`
- <a id="s-f8321326f9"></a>`signature`: `"\"(code: 'str', status: 'int') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-4aa8d99d6c"></a>`code` | `'str'` | `required` |
| <a id="s-574f983553"></a>`status` | `'int'` | `required` |

## Governing policies

- <a id="pa-527f069a92"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.HttpErrorContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9002ad25efe74a787bde23894d5da2c80fcf99464108abe9aa43aff4075945ca -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "code",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "status",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(code: 'str', status: 'int') -> None\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HttpErrorContract",
  "unit": "export"
}
```

</details>
