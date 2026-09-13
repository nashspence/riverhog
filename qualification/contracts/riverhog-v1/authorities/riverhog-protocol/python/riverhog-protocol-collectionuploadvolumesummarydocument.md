# riverhog_protocol.CollectionUploadVolumeSummaryDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadvolumes-6342ad9aa4:2e2b43f163 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-350a9f3b05"></a>
| Field | Shape |
|---|---|
| <a id="s-98f05048ff"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6cd4e3c695"></a>`distribution` | "riverhog-protocol" |
| <a id="s-d5f334369b"></a>`module` | "riverhog_protocol" |
| <a id="s-93a975870e"></a>`name` | "CollectionUploadVolumeSummaryDocument" |
| <a id="s-797c32bc18"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadVolumeSummaryDocument.validate_volume_identity](riverhog-protocol-collectionuploadvolumesummarydocument-validate-volume-identity.md)

## Governing policies

- <a id="pa-6cc1126bcd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadVolumeSummaryDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b287ef53045640f5bfc8315489f8abbd600cdd19517bb2c182fcac14eaa17a4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1ac7efaa0bbffcab4fff943fbbab4f3ed050c499a9fe1520ac38e1ccb0e6d859",
    "signature": "\"(*, volume_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:pack|segment)-[0-9a-f]{64}$', ascii_only=None)], sequence: Annotated[int, Strict(strict=True), Ge(ge=0)], kind: Literal['pack', 'segment']) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadVolumeSummaryDocument",
  "unit": "export"
}
```
