# RIVERHOG_FTP_ADAPTER_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter:riverhog-ftp-adapter-config:d5b3b9e656 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-c8cec63455) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b211a29ced"></a>
| Field | Shape |
|---|---|
| <a id="s-a33505e83b"></a>`classification` | "identity" |
| <a id="s-9c278af545"></a>`consumers` | ["riverhog-ftp-adapter"] |
| <a id="s-5a33f367a9"></a>`disposition` | "contractual" |
| <a id="s-d0c4986063"></a>`id` | "riverhog-ftp-adapter:environment:RIVERHOG_FTP_ADAPTER_CONFIG" |
| <a id="s-18bbe5ebd0"></a>`name` | "RIVERHOG_FTP_ADAPTER_CONFIG" |
| <a id="s-2f1faef8a9"></a>`owner` | "riverhog-ftp-adapter" |

## Governing policies

- <a id="pa-b665bc4da7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter:RIVERHOG_FTP_ADAPTER_CONFIG](../../../evidence/sources.md#src-67c239e18e) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/6/names` |
| parser | `riverhog-ftp-adapter` | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py` | `os.environ.get('RIVERHOG_FTP_ADAPTER_CONFIG', '')` |

### Machine authority

- `/external_contract/configuration_environment/23`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 089628a601bfb396438987edf52ca1793564b6494df34594adf1046e15a82e60 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter:environment:RIVERHOG_FTP_ADAPTER_CONFIG",
  "name": "RIVERHOG_FTP_ADAPTER_CONFIG",
  "owner": "riverhog-ftp-adapter"
}
```
