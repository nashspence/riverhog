# http_api_contracts.JSON_SEQUENCE_MEDIA_TYPE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-json-sequence-media-type:12a61e6f7e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d505295d0a"></a>
| Field | Shape |
|---|---|
| <a id="s-d320a4d58a"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-9dd965a199"></a>`distribution` | "http-api-contracts" |
| <a id="s-756975461e"></a>`module` | "http_api_contracts" |
| <a id="s-0bb83745e0"></a>`name` | "JSON_SEQUENCE_MEDIA_TYPE" |
| <a id="s-e71fd8f38b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3e46c1d6fe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.JSON_SEQUENCE_MEDIA_TYPE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e31aeb8fa8221b1f8c9a713369102755c09928f2975f858475556ca1fa8c3eb1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "application/json-seq"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "JSON_SEQUENCE_MEDIA_TYPE",
  "unit": "export"
}
```
