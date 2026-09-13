# riverhog_protocol.CollectionUploadRawDigestProgressDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawdige-ed822c7f86:69ecca9f06 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e79d55b1f"></a>
| Field | Shape |
|---|---|
| <a id="s-fa70e25928"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5d0b35929b"></a>`distribution` | "riverhog-protocol" |
| <a id="s-5dede16988"></a>`module` | "riverhog_protocol" |
| <a id="s-4cbbe4945c"></a>`name` | "CollectionUploadRawDigestProgressDocument" |
| <a id="s-9d6a624bd9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadRawDigestProgressDocument.canonical_path](riverhog-protocol-collectionuploadrawdigestprogressdocument-canonical-path.md)
- [riverhog_protocol.CollectionUploadRawDigestProgressDocument.validate_completion](riverhog-protocol-collectionuploadrawdigestprogressdocument-validate-completion.md)

## Governing policies

- <a id="pa-fb918a1fae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawDigestProgressDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2114e010268bedf5e24fbb7f0c9f5473426310fdd0434e4575a7522dc71df550 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "65617a770d19d14c6eaf53cf21c753767836d93c430135b0465dcfe6d6f219bd",
    "signature": "'(*, path: str, accepted_parts: Annotated[int, Strict(strict=True), Ge(ge=0)], expected_parts: Annotated[int, Strict(strict=True), Ge(ge=1)], complete: bool) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawDigestProgressDocument",
  "unit": "export"
}
```
