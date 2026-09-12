# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter:riverhog-base-url:f1cd39e5f6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-c8cec63455) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f3c5c446af"></a>
| Field | Shape |
|---|---|
| <a id="s-3929a8cfb9"></a>`classification` | "identity" |
| <a id="s-e5f41d64e9"></a>`consumers` | ["riverhog-ftp-adapter"] |
| <a id="s-104505ceb1"></a>`disposition` | "contractual" |
| <a id="s-4f2d62dbc3"></a>`id` | "riverhog-ftp-adapter:environment:RIVERHOG_BASE_URL" |
| <a id="s-034b439c45"></a>`name` | "RIVERHOG_BASE_URL" |
| <a id="s-5b255b15bc"></a>`owner` | "riverhog-ftp-adapter" |

## Governing policies

- <a id="pa-468fe61570"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter:RIVERHOG_BASE_URL](../../../evidence/sources.md#src-b93808cfe5) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/6/names` |
| parser | `riverhog-ftp-adapter` | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py` | `os.environ.get('RIVERHOG_BASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/22`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3346037e2b678c22671c1f990c7fc0f874f5854718628e0cec17851a67b7015 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter:environment:RIVERHOG_BASE_URL",
  "name": "RIVERHOG_BASE_URL",
  "owner": "riverhog-ftp-adapter"
}
```
