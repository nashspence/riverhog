# http_api_contracts.FRAMED_BODY_MAXIMUM_DECLARATION_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-framed-body-maximum-de-bbd36b39ef:54b5c197f4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-64969c89a6"></a>
| Field | Shape |
|---|---|
| <a id="s-d58c85ae9f"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-676f28b350"></a>`distribution` | "http-api-contracts" |
| <a id="s-185a7705d7"></a>`module` | "http_api_contracts" |
| <a id="s-6957f89ae5"></a>`name` | "FRAMED_BODY_MAXIMUM_DECLARATION_BYTES" |
| <a id="s-cab012fb47"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1553839c26"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.FRAMED_BODY_MAXIMUM_DECLARATION_BYTES`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7382824dec582c6020f3a3b418a0fa6da413de68c4538b647d7cc0ea4fbba9a2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 32768
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "FRAMED_BODY_MAXIMUM_DECLARATION_BYTES",
  "unit": "export"
}
```
