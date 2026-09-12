# RIVERHOG_LOG_LEVEL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-log-level:e95647dd31 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-53a88dfb96"></a>
| Field | Shape |
|---|---|
| <a id="s-2b3557d21d"></a>`classification` | "runtime" |
| <a id="s-cad8bbbd17"></a>`consumers` | ["riverhog-server"] |
| <a id="s-02363f5319"></a>`disposition` | "contractual" |
| <a id="s-dbe47075d5"></a>`id` | "riverhog-server:environment:RIVERHOG_LOG_LEVEL" |
| <a id="s-c305899a87"></a>`name` | "RIVERHOG_LOG_LEVEL" |
| <a id="s-02d8846ea8"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-8294662993"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_LOG_LEVEL](../../../evidence/sources.md#src-cb4528636e) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_LOG_LEVEL', DEFAULT_LOG_LEVEL)` |

### Machine authority

- `/external_contract/configuration_environment/58`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 939fd283f3043e7b6778831fecbeffaec3f047f0118febb4efc4d7dac61e175c -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_LOG_LEVEL",
  "name": "RIVERHOG_LOG_LEVEL",
  "owner": "riverhog-server"
}
```
