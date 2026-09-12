# RIVERHOG_ARCHIVE_PASSPHRASES_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-passphrases-json:ccbe989c3e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](families/credential/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1545d1acc2"></a>
| Field | Shape |
|---|---|
| <a id="s-e8bcaf1d00"></a>`classification` | "credential" |
| <a id="s-5a43b64463"></a>`consumers` | ["riverhog-server"] |
| <a id="s-fb848ed3f5"></a>`disposition` | "contractual" |
| <a id="s-84bf2d47b0"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON" |
| <a id="s-e838cafed4"></a>`name` | "RIVERHOG_ARCHIVE_PASSPHRASES_JSON" |
| <a id="s-ee0809963a"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-eb8b137898"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PASSPHRASES_JSON](../../../evidence/sources.md#src-44d728aefc) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/12/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_ARCHIVE_PASSPHRASES_JSON', '')` |

### Machine authority

- `/external_contract/configuration_environment/34`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8362577dd011f8605ac5d2f66d229f5616a4fc93df7977b451df89d5ffc6272c -->

```json
{
  "classification": "credential",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON",
  "name": "RIVERHOG_ARCHIVE_PASSPHRASES_JSON",
  "owner": "riverhog-server"
}
```
