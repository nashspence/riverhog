# riverhog_storage_adapter_protocol.DeleteObjectRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-deleteo-7b2ca9f311:5bcc5f5372 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-26e5feeb44"></a>
- <a id="s-6f37a9ac53"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-ed3e32f0ef"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-81edd6c2a5"></a>`name`: `DeleteObjectRequest`
- <a id="s-ddb332b4b6"></a>`unit`: `export`

### Declared structure

- <a id="s-ab75bbf4c8"></a>`kind`: `"class"`
- <a id="s-143da39a30"></a>`signature`: `"\"(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, mode: Literal['current', 'exact_revision', 'all_versions'], expected_current_stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""`

#### Validated model schema

<a id="s-81352d3efa"></a>
- <a id="s-16fcf7c43b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72b1581637"></a>`expected_current_stored_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-ad0ae0858d"></a>`mode` | yes | type="string"; enum=["current","exact_revision","all_versions"] |  |
| <a id="s-bfc55d9f82"></a>`object` | yes | #/$defs/ObjectLocator |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d124e0a46a"></a>`ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.DeleteObjectRequest.validate_revision](riverhog-storage-adapter-protocol-deleteobjectrequest-validate-revision.md)

## Governing policies

- <a id="pa-74a0bf9660"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.DeleteObjectRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f463b4657c0052ea70acc4a7a0834ea42f5b81224e0f79387cc09ebd685d343c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ObjectLocator": {
          "additionalProperties": false,
          "properties": {
            "object_path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "revision": {
              "anyOf": [
                {
                  "maxLength": 2000,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "object_path"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "expected_current_stored_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "mode": {
          "enum": [
            "current",
            "exact_revision",
            "all_versions"
          ],
          "type": "string"
        },
        "object": {
          "$ref": "#/$defs/ObjectLocator"
        }
      },
      "required": [
        "object",
        "mode"
      ],
      "type": "object"
    },
    "signature": "\"(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, mode: Literal['current', 'exact_revision', 'all_versions'], expected_current_stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "DeleteObjectRequest",
  "unit": "export"
}
```
