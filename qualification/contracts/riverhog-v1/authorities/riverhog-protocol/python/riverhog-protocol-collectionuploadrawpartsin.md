# riverhog_protocol.CollectionUploadRawPartsIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawpartsin:c5e09814c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be78184727"></a>
| Field | Shape |
|---|---|
| <a id="s-bb4573e450"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0d43b035c2"></a>`distribution` | "riverhog-protocol" |
| <a id="s-61b8867c02"></a>`module` | "riverhog_protocol" |
| <a id="s-b3a53c2e4e"></a>`name` | "CollectionUploadRawPartsIn" |
| <a id="s-5f32ab1388"></a>`unit` | "export" |

## Governing policies

- <a id="pa-85e0fb4666"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawPartsIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 600f5a2a5026db575ecb7ec52abdd2ac07cf7d2839149c46730cb228c8d064a2 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "76ccb435180670cd87a5f21b23e35a4dfbc75f28f4535248bc6671c05f6a8baf",
    "signature": "\"(*, part_plaintext_bytes: Annotated[int, Ge(ge=65536)], part_count: Annotated[int, Strict(strict=True), Ge(ge=1)], ordered_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawPartsIn",
  "unit": "export"
}
```
