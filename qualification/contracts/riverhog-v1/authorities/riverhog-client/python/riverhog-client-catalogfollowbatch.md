# riverhog_client.CatalogFollowBatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowbatch:119074c9a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe6f20da64"></a>
- <a id="s-e47419ad10"></a>`distribution`: `riverhog-client`
- <a id="s-e7de3f0c82"></a>`module`: `riverhog_client`
- <a id="s-a7e1fdb2da"></a>`name`: `CatalogFollowBatch`
- <a id="s-83a931c0a7"></a>`unit`: `export`

### Declared structure

- <a id="s-4ad1ec5173"></a>`kind`: `"class"`
- <a id="s-f968e4e74f"></a>`signature`: `"\"(kind: 'CatalogFollowKind', before: 'CatalogFollowPosition', after: 'CatalogFollowPosition', collections: 'tuple[CatalogSyncDescriptor, ...]' = (), changes: 'tuple[CatalogSyncUpsert \| CatalogSyncDeparture, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-5015081223"></a>`kind` | `'CatalogFollowKind'` | `required` |
| <a id="s-c6a60cfff9"></a>`before` | `'CatalogFollowPosition'` | `required` |
| <a id="s-935cf8b4d7"></a>`after` | `'CatalogFollowPosition'` | `required` |
| <a id="s-e172e35aa0"></a>`collections` | `'tuple[CatalogSyncDescriptor, ...]'` | `()` |
| <a id="s-d54e561ef4"></a>`changes` | `'tuple[CatalogSyncUpsert \| CatalogSyncDeparture, ...]'` | `()` |

## Governing policies

- <a id="pa-349b25fd44"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowBatch`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da133c4662f6667f6c5fcee163651dec3e53890c5912f045ce4a2f135069c0f7 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "kind",
        "type": "'CatalogFollowKind'"
      },
      {
        "default": "required",
        "name": "before",
        "type": "'CatalogFollowPosition'"
      },
      {
        "default": "required",
        "name": "after",
        "type": "'CatalogFollowPosition'"
      },
      {
        "default": "()",
        "name": "collections",
        "type": "'tuple[CatalogSyncDescriptor, ...]'"
      },
      {
        "default": "()",
        "name": "changes",
        "type": "'tuple[CatalogSyncUpsert | CatalogSyncDeparture, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(kind: 'CatalogFollowKind', before: 'CatalogFollowPosition', after: 'CatalogFollowPosition', collections: 'tuple[CatalogSyncDescriptor, ...]' = (), changes: 'tuple[CatalogSyncUpsert | CatalogSyncDeparture, ...]' = ()) -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogFollowBatch",
  "unit": "export"
}
```

</details>
