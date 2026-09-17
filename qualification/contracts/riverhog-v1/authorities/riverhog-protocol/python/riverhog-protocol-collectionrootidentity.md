# riverhog_protocol.CollectionRootIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionrootidentity:0882ec68f4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ecd2675017"></a>
- <a id="s-d2c3f0b061"></a>`distribution`: `riverhog-protocol`
- <a id="s-6e22e81352"></a>`module`: `riverhog_protocol`
- <a id="s-cc073b2de5"></a>`name`: `CollectionRootIdentity`
- <a id="s-8b5c9e5708"></a>`unit`: `export`

### Declared structure

- <a id="s-74775589eb"></a>`kind`: `"class"`
- <a id="s-e22235febd"></a>`signature`: `"\"(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-44f7a9bc0f"></a>`collection_id` | `'CollectionId'` | `required` |
| <a id="s-73a0e570bf"></a>`archive_root_sha256` | `'str'` | `required` |
| <a id="s-f0509387af"></a>`content_identity` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [as_dict](riverhog-protocol-collectionrootidentity-as-dict.md)
- [from_mapping](riverhog-protocol-collectionrootidentity-from-mapping.md)

## Governing policies

- <a id="pa-a3ddfa3f54"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionRootIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1990d43a34f952e86cf19ccd5eccf630884b5eb34971d123ff441f22e0c0cf38 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "collection_id",
        "type": "'CollectionId'"
      },
      {
        "default": "required",
        "name": "archive_root_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "content_identity",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionRootIdentity",
  "unit": "export"
}
```

</details>
