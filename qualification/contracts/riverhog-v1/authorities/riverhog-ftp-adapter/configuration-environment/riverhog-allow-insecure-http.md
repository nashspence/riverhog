# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter:riverhog-allow-insecure-http:bcb0c36de8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-7da220a9da) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-20d8c220d5"></a>
| Field | Shape |
|---|---|
| <a id="s-e6b0f79731"></a>`classification` | "runtime" |
| <a id="s-afe2b91816"></a>`consumers` | ["riverhog-ftp-adapter"] |
| <a id="s-de4fe39928"></a>`disposition` | "contractual" |
| <a id="s-26e4801367"></a>`id` | "riverhog-ftp-adapter:environment:RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-c40791ce1d"></a>`name` | "RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-e151128158"></a>`owner` | "riverhog-ftp-adapter" |

## Governing policies

- <a id="pa-cfb1692136"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-8be0f96180) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/7/names` |
| parser | `riverhog-ftp-adapter` | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py` | `os.environ.get('RIVERHOG_ALLOW_INSECURE_HTTP', 'false')` |

### Machine authority

- `/external_contract/configuration_environment/21`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a1f5fa5688b5bca2000d3404ccce0d471b80ee4ceaf70bcaba2e7732dfe017f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter:environment:RIVERHOG_ALLOW_INSECURE_HTTP",
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP",
  "owner": "riverhog-ftp-adapter"
}
```
