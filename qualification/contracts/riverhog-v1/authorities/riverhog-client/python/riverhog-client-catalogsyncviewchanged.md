# riverhog_client.CatalogSyncViewChanged

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogsyncviewchanged:fc8a251dac -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-289760295f"></a>
| Field | Shape |
|---|---|
| <a id="s-c90ccaba39"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c03e40bddf"></a>`distribution` | "riverhog-client" |
| <a id="s-275b1cabd4"></a>`module` | "riverhog_client" |
| <a id="s-8550db7794"></a>`name` | "CatalogSyncViewChanged" |
| <a id="s-9f35ebedd2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3781ba322e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogSyncViewChanged`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ff28ee7924e4ec3121c47e21e36281812aa1bc08b2561ec50146c28f180cf58 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogSyncViewChanged",
  "unit": "export"
}
```
