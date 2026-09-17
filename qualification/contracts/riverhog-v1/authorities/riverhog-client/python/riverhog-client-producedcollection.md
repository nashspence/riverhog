# riverhog_client.ProducedCollection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-producedcollection:62ccea471c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-02a57ac2ae"></a>
- <a id="s-9c8ae74029"></a>`distribution`: `riverhog-client`
- <a id="s-d873aed8ea"></a>`module`: `riverhog_client`
- <a id="s-923485e9e5"></a>`name`: `ProducedCollection`
- <a id="s-3a1d362daa"></a>`unit`: `export`

### Declared structure

- <a id="s-bae50fad2d"></a>`kind`: `"class"`
- <a id="s-b33322b666"></a>`signature`: `"\"(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str', receipt: 'dict[str, Any]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-281850063c"></a>`collection_id` | `'CollectionId'` | `required` |
| <a id="s-643cc3655a"></a>`archive_root_sha256` | `'str'` | `required` |
| <a id="s-f35429893d"></a>`content_identity` | `'str'` | `required` |
| <a id="s-aa9d38b2ce"></a>`receipt` | `'dict[str, Any]'` | `required` |

## Governing policies

- <a id="pa-f842b8530f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.ProducedCollection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37cbcd523e6f9906c9499265da21b1791db1b7dd4a824a8af047e1811f3efbb5 -->

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
      },
      {
        "default": "required",
        "name": "receipt",
        "type": "'dict[str, Any]'"
      }
    ],
    "kind": "class",
    "signature": "\"(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str', receipt: 'dict[str, Any]') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ProducedCollection",
  "unit": "export"
}
```

</details>
