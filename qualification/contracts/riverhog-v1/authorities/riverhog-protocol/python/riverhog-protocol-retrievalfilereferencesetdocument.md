# riverhog_protocol.RetrievalFileReferenceSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferencesetdocument:87cb8658a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1115155719"></a>
| Field | Shape |
|---|---|
| <a id="s-e5ae605562"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-59bf0e8cee"></a>`distribution` | "riverhog-protocol" |
| <a id="s-1c628d835b"></a>`module` | "riverhog_protocol" |
| <a id="s-a2a199460a"></a>`name` | "RetrievalFileReferenceSetDocument" |
| <a id="s-ce9fd8f62c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RetrievalFileReferenceSetDocument.validate_exact_reference_set](riverhog-protocol-retrievalfilereferencesetdocument-validate-exact-reference-set.md)

## Governing policies

- <a id="pa-5fe3c33ae8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceSetDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 954c9be129114059139a83f6cfc8182fc12949fc124e934699873331f234c251 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4c11764dce1fcd90fb2ef4ea211c8ba1ae3c76a21a1cf46e720fd2d17d827e3c",
    "signature": "'(*, files: Annotated[list[riverhog_protocol.retrieval_transport.RetrievalFileReferenceDocument], MinLen(min_length=1), MaxLen(max_length=10000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalFileReferenceSetDocument",
  "unit": "export"
}
```
