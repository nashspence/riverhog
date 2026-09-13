# RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-cursor-lifetime:635b93e8a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ef146f919d"></a>
| Field | Shape |
|---|---|
| <a id="s-93a23b729e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-4a3b51c44c"></a>`default_expressions` | ["'24h'"] |
| <a id="s-7df70bf7e9"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME" |
| <a id="s-202888fb55"></a>`input_shape` | "environment-string" |
| <a id="s-4e7ddbc9fe"></a>`name` | "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME" |
| <a id="s-3e6453e97a"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-cc49c039c0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../../../evidence/sources.md#src-afa71403d4) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/52`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72c266eb336e70afa2436ce48d879e5d129ea88a9e1a1eb3841a56236a27dd24 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'24h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "owner": "riverhog-server"
}
```
