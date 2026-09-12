# RIVERHOG_PUBLIC_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-public-base-url:8b72c2dec7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](families/identity/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bff615c87e"></a>
| Field | Shape |
|---|---|
| <a id="s-a3dcea55b0"></a>`classification` | "identity" |
| <a id="s-b6aa311db5"></a>`consumers` | ["riverhog-server"] |
| <a id="s-d35248f247"></a>`disposition` | "contractual" |
| <a id="s-27e6fb1e12"></a>`id` | "riverhog-server:environment:RIVERHOG_PUBLIC_BASE_URL" |
| <a id="s-6be1a5a47c"></a>`name` | "RIVERHOG_PUBLIC_BASE_URL" |
| <a id="s-cf178005dd"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-70efb3f9a7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_PUBLIC_BASE_URL](../../../evidence/sources.md#src-fbbd173a78) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/13/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_PUBLIC_BASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/62`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1548824f8fe7fd8aa61876f589fe0cd76f1006613ff0e90cbba4feffb782d6ea -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_PUBLIC_BASE_URL",
  "name": "RIVERHOG_PUBLIC_BASE_URL",
  "owner": "riverhog-server"
}
```
