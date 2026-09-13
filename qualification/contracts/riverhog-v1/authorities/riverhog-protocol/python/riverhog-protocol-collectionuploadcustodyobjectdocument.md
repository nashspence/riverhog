# riverhog_protocol.CollectionUploadCustodyObjectDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadcustody-426d457c69:21bbd11981 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-513c81b23d"></a>
| Field | Shape |
|---|---|
| <a id="s-88e4054d80"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f48abc51d8"></a>`distribution` | "riverhog-protocol" |
| <a id="s-7a030f78e6"></a>`module` | "riverhog_protocol" |
| <a id="s-c50bf567cc"></a>`name` | "CollectionUploadCustodyObjectDocument" |
| <a id="s-45b93e6f41"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c3f42dcd81"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadCustodyObjectDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ba59fa1c9a331a3e83d7ec8a8217a7d39248c2e9972df20ccba068b54975eae -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ab739b34e762d74a982f0188a94fe8c54345a38aee2272388a4382dd8deebd29",
    "signature": "\"(*, volume_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:pack|segment)-[0-9a-f]{64}$', ascii_only=None)], sealed_receipt_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadCustodyObjectDocument",
  "unit": "export"
}
```
