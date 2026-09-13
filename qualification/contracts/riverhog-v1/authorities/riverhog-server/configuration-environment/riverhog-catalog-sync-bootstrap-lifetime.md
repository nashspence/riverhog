# RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-bootstrap-lifetime:2f5209b270 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9857458ffa"></a>
| Field | Shape |
|---|---|
| <a id="s-fe693377ae"></a>`consumers` | ["riverhog-server"] |
| <a id="s-0473aebcab"></a>`default_expressions` | ["'7d'"] |
| <a id="s-8cbdf403cb"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME" |
| <a id="s-7b2e5c71b6"></a>`input_shape` | "environment-string" |
| <a id="s-03c589148c"></a>`name` | "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME" |
| <a id="s-0666993cd6"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-419f97cb39"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME](../../../evidence/sources.md#src-bb4e9ed43a) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME', '7d')` |

### Machine authority

- `/external_contract/configuration_environment/51`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60a9369be20017e0e474fec863c8b1dd23a29cb1bb68609dc4ce70a90574fe94 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'7d'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME",
  "owner": "riverhog-server"
}
```
