# riverhog_client.CatalogSyncCursorExpired

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogsynccursorexpired:27aa9e9ff9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45288221b1"></a>
| Field | Shape |
|---|---|
| <a id="s-13abed3305"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-52a182446a"></a>`distribution` | "riverhog-client" |
| <a id="s-30ea5a66c8"></a>`module` | "riverhog_client" |
| <a id="s-d294d05d69"></a>`name` | "CatalogSyncCursorExpired" |
| <a id="s-a4b481836b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-776873e370"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogSyncCursorExpired`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0cadc4dff4f8af070099c678c0b5f68101a2c4ca2abfdb86de1c6c53a3997400 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogSyncCursorExpired",
  "unit": "export"
}
```
