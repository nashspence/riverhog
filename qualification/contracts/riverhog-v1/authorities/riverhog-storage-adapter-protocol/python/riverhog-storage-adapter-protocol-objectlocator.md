# riverhog_storage_adapter_protocol.ObjectLocator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectlocator:e847ba3146 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac76af497d"></a>
- <a id="s-31923cc8a4"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-0017528ac7"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-dde4b58abe"></a>`name`: `ObjectLocator`
- <a id="s-9fdc9f1df3"></a>`unit`: `export`

### Declared structure

- <a id="s-309dd7fce3"></a>`kind`: `"class"`
- <a id="s-42065b5716"></a>`signature`: `"'(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=2000)] = None) -> None'"`

#### Validated model schema

<a id="s-d5b5deaef9"></a>
- <a id="s-7c1e8986f2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4623f87320"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-7ef7be1c5b"></a>`revision` | no | anyOf=type="string"; minLength=1; maxLength=2000 \| type="null" |  |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ObjectLocator.canonical_path](riverhog-storage-adapter-protocol-objectlocator-canonical-path.md)

## Governing policies

- <a id="pa-0c04b8db4c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectLocator`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42e4374de048bfd72fa0f1ffa830ec85ddd1ac6adec81fccde6e4d26b955bc6d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
    },
    "signature": "'(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectLocator",
  "unit": "export"
}
```
