# RIVERHOG_FTP_ADAPTER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-token:152609ed91 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-e056120d8e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d7a4e6e0e8"></a>
| Field | Shape |
|---|---|
| <a id="s-1f968da003"></a>`classification` | "credential" |
| <a id="s-a5e7a20adb"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-2eb73f8083"></a>`disposition` | "contractual" |
| <a id="s-f2739e6aac"></a>`id` | "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_TOKEN" |
| <a id="s-495c4fd74d"></a>`name` | "RIVERHOG_FTP_ADAPTER_TOKEN" |
| <a id="s-eba1397ffc"></a>`owner` | "riverhog-ftp-adapter-api-client" |

## Governing policies

- <a id="pa-9c997ebebe"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_TOKEN](../../../evidence/sources.md#src-5a52b017b3) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/8/names` |
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `os.getenv('RIVERHOG_FTP_ADAPTER_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/28`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1d8d6dbaa29e92069de1149ca3d3a9c601519ebbb6abc02a4ce8aa7851f8b39 -->

```json
{
  "classification": "credential",
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_TOKEN",
  "name": "RIVERHOG_FTP_ADAPTER_TOKEN",
  "owner": "riverhog-ftp-adapter-api-client"
}
```
