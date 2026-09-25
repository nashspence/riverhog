# riverhog_client.CatalogFollowApi

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowapi:8131bb429f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7cf72dffa3"></a>
- <a id="s-651bdc3af6"></a>`distribution`: `riverhog-client`
- <a id="s-bff79fec31"></a>`module`: `riverhog_client`
- <a id="s-cbda9b4fe6"></a>`name`: `CatalogFollowApi`
- <a id="s-0133182011"></a>`unit`: `export`

### Declared structure

- <a id="s-c4da87c163"></a>`kind`: `"class"`
- <a id="s-5900dec2c4"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [create_catalog_sync_checkpoint](riverhog-client-catalogfollowapi-create-catalog-sync-checkpoint.md)
- [list_catalog_sync_collections](riverhog-client-catalogfollowapi-list-catalog-sync-collections.md)
- [list_catalog_sync_changes](riverhog-client-catalogfollowapi-list-catalog-sync-changes.md)

## Governing policies

- <a id="pa-9f2676ee48"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowApi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b0f8b965f4e8428e555f3b3395afd90135128de496f0cdc82b80ae225bc8493 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogFollowApi",
  "unit": "export"
}
```

</details>
