# riverhog_protocol.CollectionUploadUnitWorkDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitworkdocument:5f3207bf12 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dab1662726"></a>
| Field | Shape |
|---|---|
| <a id="s-131345231b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2e33e51c02"></a>`distribution` | "riverhog-protocol" |
| <a id="s-b98beb79bc"></a>`module` | "riverhog_protocol" |
| <a id="s-ae8145032e"></a>`name` | "CollectionUploadUnitWorkDocument" |
| <a id="s-e700e9fd05"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2918ee7ee4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitWorkDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd8567500348852b47a732c0b9425b4b654ddb9389e7001aff57626399afd72f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5d13849f70a7f586536a938620d35d29a3d9ed0f21b372227d37457467cbc83a",
    "signature": "\"(*, unit: Annotated[int, Ge(ge=0)], payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], plaintext_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)], state: Literal['pending', 'committed']) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitWorkDocument",
  "unit": "export"
}
```
