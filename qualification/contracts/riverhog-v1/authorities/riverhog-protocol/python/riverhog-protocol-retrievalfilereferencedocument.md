# riverhog_protocol.RetrievalFileReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferencedocument:eb093ca910 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7002b79901"></a>
| Field | Shape |
|---|---|
| <a id="s-aa711bd2d7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-444f550f0a"></a>`distribution` | "riverhog-protocol" |
| <a id="s-2f3c197753"></a>`module` | "riverhog_protocol" |
| <a id="s-612384c044"></a>`name` | "RetrievalFileReferenceDocument" |
| <a id="s-415503c74c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f8c9fe0c85"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3dde1b32a9fb783e814ce62e6c9230f5d2fd81a8d86ab65f29367a428487621 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "46c0a44ede7dac165b09955455c01f3facbf8b911f1ebc4472f6653e8348ea57",
    "signature": "'(*, collection_id: CollectionId, path: CanonicalRelPath) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalFileReferenceDocument",
  "unit": "export"
}
```
