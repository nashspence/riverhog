# riverhog_storage_adapter_protocol.StorageAdapterError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-2d8aa31d10:20ca0a401e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-339d4e2876"></a>
- <a id="s-1df02dc29f"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-b959e15e1d"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2fd6a49f10"></a>`name`: `StorageAdapterError`
- <a id="s-69541978c7"></a>`unit`: `export`

### Declared structure

- <a id="s-8b53d2c079"></a>`kind`: `"class"`
- <a id="s-c6baa213d9"></a>`signature`: `"'(*, error: riverhog_storage_adapter_protocol.protocol.StorageAdapterErrorBody) -> None'"`

#### Validated model schema

<a id="s-8273a3b582"></a>
- <a id="s-0995432c5d"></a>`title`: StorageAdapterError
- <a id="s-60f4870d95"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3f94044d27"></a>`error` | yes | #/$defs/StorageAdapterErrorBody |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-1ac506b6c2"></a>`StorageAdapterErrorBody` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-44a4df69cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f8401df24b68a7b7eb412fa030ee60ce425eb6fc8bf0a4aedb132be4508b262 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "StorageAdapterErrorBody": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "enum": [
                "unauthorized",
                "invalid_request",
                "not_found",
                "method_not_allowed",
                "length_required",
                "request_too_large",
                "insufficient_storage",
                "identity_conflict",
                "traversal_invalidated",
                "invalid_path",
                "invalid_range",
                "read_not_ready",
                "read_expired",
                "integrity_failure",
                "provider_unavailable",
                "internal_failure"
              ],
              "title": "Code",
              "type": "string"
            },
            "message": {
              "maxLength": 2000,
              "minLength": 1,
              "title": "Message",
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "title": "StorageAdapterErrorBody",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "error": {
          "$ref": "#/$defs/StorageAdapterErrorBody"
        }
      },
      "required": [
        "error"
      ],
      "title": "StorageAdapterError",
      "type": "object"
    },
    "signature": "'(*, error: riverhog_storage_adapter_protocol.protocol.StorageAdapterErrorBody) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "StorageAdapterError",
  "unit": "export"
}
```
