# riverhog_protocol.CollectionUploadRawDigestBatchDocument.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawdige-9340bc5b99:c7bf85b24e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-28988f6a13"></a>
| Field | Shape |
|---|---|
| <a id="s-5b349aa0dc"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-31ef474875"></a>`distribution` | "riverhog-protocol" |
| <a id="s-98ef06b30a"></a>`module` | "riverhog_protocol" |
| <a id="s-37f623a7ed"></a>`name` | "canonical_path" |
| <a id="s-79d2914ef9"></a>`owner` | "riverhog_protocol.CollectionUploadRawDigestBatchDocument" |
| <a id="s-ce65165d13"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadRawDigestBatchDocument](riverhog-protocol-collectionuploadrawdigestbatchdocument.md)

## Governing policies

- <a id="pa-cf598a119d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawDigestBatchDocument.canonical_path`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79e0301846683c9162efa211d5c2b9bf37dd3d3de0c20f1c16b58d97a42a7e26 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "canonical_path",
  "owner": "riverhog_protocol.CollectionUploadRawDigestBatchDocument",
  "unit": "member"
}
```
