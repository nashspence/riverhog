# riverhog_protocol.CatalogSyncIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncidentity:31f5368d75 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8809525b09"></a>
| Field | Shape |
|---|---|
| <a id="s-b6f8c2be00"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-e1afb7e2eb"></a>`distribution` | "riverhog-protocol" |
| <a id="s-68e7dce190"></a>`module` | "riverhog_protocol" |
| <a id="s-dd1b60239e"></a>`name` | "CatalogSyncIdentity" |
| <a id="s-0928275e5a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-35f5be7568"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77265d5bf7fb81b704c9d6f9df6899175ef6cafe6d1345b3682d6cbfb5a0bd07 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncIdentity",
  "unit": "export"
}
```
