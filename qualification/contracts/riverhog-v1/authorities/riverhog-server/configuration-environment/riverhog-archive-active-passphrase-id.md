# RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-active-passphrase-id:77c1645717 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](families/identity/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a9fb2eff34"></a>
| Field | Shape |
|---|---|
| <a id="s-54a69f2242"></a>`classification` | "identity" |
| <a id="s-c00038c38f"></a>`consumers` | ["riverhog-server"] |
| <a id="s-153c15d3a1"></a>`disposition` | "contractual" |
| <a id="s-c9b454920d"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID" |
| <a id="s-caff9576d7"></a>`name` | "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID" |
| <a id="s-69d21c7a53"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-48fcff73d4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID](../../../evidence/sources.md#src-59ce1b7699) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/13/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID', '')` |

### Machine authority

- `/external_contract/configuration_environment/32`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 819ac06cc86308a326869546691a4666c4e20089b150cdb09e0e40a72b8af716 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID",
  "name": "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID",
  "owner": "riverhog-server"
}
```
