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
- <a id="s-f48abc51d8"></a>`distribution`: `riverhog-protocol`
- <a id="s-7a030f78e6"></a>`module`: `riverhog_protocol`
- <a id="s-c50bf567cc"></a>`name`: `CollectionUploadCustodyObjectDocument`
- <a id="s-45b93e6f41"></a>`unit`: `export`

### Declared structure

- <a id="s-06ea8874f0"></a>`kind`: `"class"`
- <a id="s-6da940d917"></a>`signature`: `"\"(*, volume_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:pack\|segment)-[0-9a-f]{64}$', ascii_only=None)], sealed_receipt_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-dc86bf46a1"></a>
- <a id="s-5ce7ee6e4a"></a>`title`: CollectionUploadCustodyObjectDocument
- <a id="s-a190c457bb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cea73fb356"></a>`sealed_receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f6371e9790"></a>`volume_id` | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$" |  |

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

<!-- exact-contract-value: c17e3c5cede438a8330c84545889a4a07c4eeff29c01d912cae5ec023ee309d0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "sealed_receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sealed Receipt Sha256",
          "type": "string"
        },
        "volume_id": {
          "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
          "title": "Volume Id",
          "type": "string"
        }
      },
      "required": [
        "volume_id",
        "sealed_receipt_sha256"
      ],
      "title": "CollectionUploadCustodyObjectDocument",
      "type": "object"
    },
    "signature": "\"(*, volume_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:pack|segment)-[0-9a-f]{64}$', ascii_only=None)], sealed_receipt_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadCustodyObjectDocument",
  "unit": "export"
}
```
