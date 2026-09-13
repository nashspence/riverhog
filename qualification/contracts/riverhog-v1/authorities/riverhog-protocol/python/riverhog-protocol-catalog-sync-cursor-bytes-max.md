# riverhog_protocol.CATALOG_SYNC_CURSOR_BYTES_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalog-sync-cursor-bytes-max:5e3be1cba7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43fb4ddb8e"></a>
| Field | Shape |
|---|---|
| <a id="s-93320f028b"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-d2129b7889"></a>`distribution` | "riverhog-protocol" |
| <a id="s-4beb636087"></a>`module` | "riverhog_protocol" |
| <a id="s-17009523fd"></a>`name` | "CATALOG_SYNC_CURSOR_BYTES_MAX" |
| <a id="s-ca92f4e99b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8b8937ba71"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CATALOG_SYNC_CURSOR_BYTES_MAX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dcc613c7361d36b4b695889e5339f9aa12f86b078f2c995c53dc6a2322fb99fb -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 4096
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CATALOG_SYNC_CURSOR_BYTES_MAX",
  "unit": "export"
}
```
