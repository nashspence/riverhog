# riverhog_protocol.CATALOG_SYNC_PAGE_SIZE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalog-sync-page-size-max:5717035523 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f41bb89cb"></a>
| Field | Shape |
|---|---|
| <a id="s-810cb9ac26"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-8e40df5e43"></a>`distribution` | "riverhog-protocol" |
| <a id="s-75b3917656"></a>`module` | "riverhog_protocol" |
| <a id="s-7b0a7c64b1"></a>`name` | "CATALOG_SYNC_PAGE_SIZE_MAX" |
| <a id="s-27ee5f153c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d38dd4326d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CATALOG_SYNC_PAGE_SIZE_MAX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56a63795e601d8432b510b689f923595f0e0228cc44895e5221b9bb372620617 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 100
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CATALOG_SYNC_PAGE_SIZE_MAX",
  "unit": "export"
}
```
