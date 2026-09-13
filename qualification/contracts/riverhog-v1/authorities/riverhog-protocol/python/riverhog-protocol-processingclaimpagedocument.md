# riverhog_protocol.ProcessingClaimPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimpagedocument:0157b994c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-885d7fc9be"></a>
| Field | Shape |
|---|---|
| <a id="s-3e8fe06898"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2e64bcc8f5"></a>`distribution` | "riverhog-protocol" |
| <a id="s-8ab1096825"></a>`module` | "riverhog_protocol" |
| <a id="s-c6423f7606"></a>`name` | "ProcessingClaimPageDocument" |
| <a id="s-22e70d1800"></a>`unit` | "export" |

## Governing policies

- <a id="pa-efe7905141"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPageDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5fa6fbbc2a57869670653948644869585b421fc765c8ae9705cca7e80a797b8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9d245807b3d4027ff191a556a26abf6a055123c28625eb457786f75687e274a0",
    "signature": "'(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: ProcessingClaimSort, order: SortOrder, filters: riverhog_protocol.collection_workflow_transport.ProcessingClaimFiltersDocument, claims: list[riverhog_protocol.collection_workflow_transport.ProcessingClaimDocument]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimPageDocument",
  "unit": "export"
}
```
