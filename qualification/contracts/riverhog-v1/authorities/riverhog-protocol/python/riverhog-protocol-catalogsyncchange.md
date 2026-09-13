# riverhog_protocol.CatalogSyncChange

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncchange:268324e631 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-87903cd901"></a>
| Field | Shape |
|---|---|
| <a id="s-1bf3a42ab7"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-f4f7e36bd4"></a>`distribution` | "riverhog-protocol" |
| <a id="s-9d602ec53f"></a>`module` | "riverhog_protocol" |
| <a id="s-34e7d26443"></a>`name` | "CatalogSyncChange" |
| <a id="s-261fcb2f21"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0a6234d998"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncChange`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d95dc0d27dfbd0953d12e8209299f19e86fae40f080c476c346b0df23cee9bda -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncChange",
  "unit": "export"
}
```
