# riverhog_client.CatalogFollowPosition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowposition:07e271d6f2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e594e6b73"></a>
- <a id="s-45d7225dc5"></a>`distribution`: `riverhog-client`
- <a id="s-486e10e08f"></a>`module`: `riverhog_client`
- <a id="s-aaae37d1ce"></a>`name`: `CatalogFollowPosition`
- <a id="s-5eb4ed9d5d"></a>`unit`: `export`

### Declared structure

- <a id="s-66f2c49867"></a>`kind`: `"class"`
- <a id="s-932e7f9171"></a>`signature`: `"\"(phase: 'CatalogFollowPhase' = 'new', source_identity: 'str \| None' = None, authorization_view_identity: 'str \| None' = None, cursor: 'str \| None' = None, through_revision: 'str' = '0', last_collection_id: 'int \| None' = None, reset_reason: 'str \| None' = None) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-cca53525e9"></a>`phase` | `'CatalogFollowPhase'` | `'new'` |
| <a id="s-ca32932094"></a>`source_identity` | `'str \| None'` | `None` |
| <a id="s-5ac5a60407"></a>`authorization_view_identity` | `'str \| None'` | `None` |
| <a id="s-f18e92284f"></a>`cursor` | `'str \| None'` | `None` |
| <a id="s-e2b96eeb56"></a>`through_revision` | `'str'` | `'0'` |
| <a id="s-ef6072be8d"></a>`last_collection_id` | `'int \| None'` | `None` |
| <a id="s-2341b1dc5b"></a>`reset_reason` | `'str \| None'` | `None` |

## Governing policies

- <a id="pa-18e75cacee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowPosition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20b29cb24d7794afb6d9bdd9a97cce1a54b4ef82bf71348d258ec055e41990c3 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "'new'",
        "name": "phase",
        "type": "'CatalogFollowPhase'"
      },
      {
        "default": "None",
        "name": "source_identity",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "authorization_view_identity",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "cursor",
        "type": "'str | None'"
      },
      {
        "default": "'0'",
        "name": "through_revision",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "last_collection_id",
        "type": "'int | None'"
      },
      {
        "default": "None",
        "name": "reset_reason",
        "type": "'str | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(phase: 'CatalogFollowPhase' = 'new', source_identity: 'str | None' = None, authorization_view_identity: 'str | None' = None, cursor: 'str | None' = None, through_revision: 'str' = '0', last_collection_id: 'int | None' = None, reset_reason: 'str | None' = None) -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogFollowPosition",
  "unit": "export"
}
```

</details>
