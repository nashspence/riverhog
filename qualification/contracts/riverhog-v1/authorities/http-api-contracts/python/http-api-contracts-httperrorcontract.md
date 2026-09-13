# http_api_contracts.HttpErrorContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httperrorcontract:8e607bcede -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f620769d5"></a>
| Field | Shape |
|---|---|
| <a id="s-86d8c65836"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-e6ec0189b3"></a>`distribution` | "http-api-contracts" |
| <a id="s-f16772f287"></a>`module` | "http_api_contracts" |
| <a id="s-9aca6d0207"></a>`name` | "HttpErrorContract" |
| <a id="s-a7684f9b74"></a>`unit` | "export" |

## Governing policies

- <a id="pa-527f069a92"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpErrorContract`

### Exact owned JSON

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
