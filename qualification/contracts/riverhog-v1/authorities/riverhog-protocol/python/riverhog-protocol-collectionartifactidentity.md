# riverhog_protocol.CollectionArtifactIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionartifactidentity:ade7d15ef4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-797af3730b"></a>
- <a id="s-1c25450659"></a>`distribution`: `riverhog-protocol`
- <a id="s-4c920a11d6"></a>`module`: `riverhog_protocol`
- <a id="s-b454b11c85"></a>`name`: `CollectionArtifactIdentity`
- <a id="s-24191890fd"></a>`unit`: `export`

### Declared structure

- <a id="s-2c7db9a5a7"></a>`kind`: `"class"`
- <a id="s-ba6342923c"></a>`signature`: `"\"(collection: 'CollectionRootIdentity', path: 'str', bytes: 'int', sha256: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-5746e84cf0"></a>`collection` | `'CollectionRootIdentity'` | `required` |
| <a id="s-4f565f805e"></a>`path` | `'str'` | `required` |
| <a id="s-cdf51b8821"></a>`bytes` | `'int'` | `required` |
| <a id="s-a54be9fa50"></a>`sha256` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionArtifactIdentity.from_mapping](riverhog-protocol-collectionartifactidentity-from-mapping.md)
- [riverhog_protocol.CollectionArtifactIdentity.as_dict](riverhog-protocol-collectionartifactidentity-as-dict.md)

## Governing policies

- <a id="pa-f2c5d233c7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionArtifactIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6545dcf073ddc8cd894791ad2489db32208f0261fb6abb2a0cd9ee212c876029 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "collection",
        "type": "'CollectionRootIdentity'"
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(collection: 'CollectionRootIdentity', path: 'str', bytes: 'int', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionArtifactIdentity",
  "unit": "export"
}
```
