# riverhog_storage_adapter_protocol.DeletePrefixRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-deletep-cf1b6760cd:e0041a9c72 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b80efa5439"></a>
- <a id="s-956f06853e"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-be1745cefe"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-07c093b23c"></a>`name`: `DeletePrefixRequest`
- <a id="s-de499e6350"></a>`unit`: `export`

### Declared structure

- <a id="s-7c784c3782"></a>`kind`: `"class"`
- <a id="s-c046c17ec7"></a>`signature`: `"\"(*, object_prefix: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], mode: Literal['all_versions'] = 'all_versions') -> None\""`

#### Validated model schema

<a id="s-4af80adaa0"></a>
- <a id="s-3457bf0d8d"></a>`title`: DeletePrefixRequest
- <a id="s-f3aad3559f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-adc71a7522"></a>`mode` | no | type="string"; const="all_versions" |  |
| <a id="s-e8c37cc1f2"></a>`object_prefix` | yes | type="string"; minLength=1; maxLength=4096 |  |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.DeletePrefixRequest.canonical_prefix](riverhog-storage-adapter-protocol-deleteprefixrequest-canonical-prefix.md)

## Governing policies

- <a id="pa-99fd68a241"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.DeletePrefixRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 995d8db3b0e81641553113660118501ef99e34883df8d32464d0213c759316a9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "mode": {
          "const": "all_versions",
          "default": "all_versions",
          "title": "Mode",
          "type": "string"
        },
        "object_prefix": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Object Prefix",
          "type": "string"
        }
      },
      "required": [
        "object_prefix"
      ],
      "title": "DeletePrefixRequest",
      "type": "object"
    },
    "signature": "\"(*, object_prefix: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], mode: Literal['all_versions'] = 'all_versions') -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "DeletePrefixRequest",
  "unit": "export"
}
```
