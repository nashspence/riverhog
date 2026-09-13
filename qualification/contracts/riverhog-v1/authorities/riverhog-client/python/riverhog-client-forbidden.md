# riverhog_client.Forbidden

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-forbidden:fd9da0272a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e179ac78a"></a>
| Field | Shape |
|---|---|
| <a id="s-87a39a0400"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ad097beebd"></a>`distribution` | "riverhog-client" |
| <a id="s-605b8c0f4e"></a>`module` | "riverhog_client" |
| <a id="s-ab63baccbc"></a>`name` | "Forbidden" |
| <a id="s-8a6f66c72a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4af7fe8e65"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.Forbidden`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bed33452536d07300fbd322c7c77f8e0f26eb2f0102366e13c25ac75738bac38 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "Forbidden",
  "unit": "export"
}
```
