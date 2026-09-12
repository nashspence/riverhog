# STOVE0_WORKSPACE_ASSURANCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-workspace-assurance:64f37fc577 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-25dea1f42a"></a>
| Field | Shape |
|---|---|
| <a id="s-433920d1a6"></a>`classification` | "runtime" |
| <a id="s-648007d91b"></a>`consumers` | ["stove0-server"] |
| <a id="s-2c10d22728"></a>`disposition` | "contractual" |
| <a id="s-8ff65ad249"></a>`id` | "stove0-server:environment:STOVE0_WORKSPACE_ASSURANCE" |
| <a id="s-79164d1f48"></a>`name` | "STOVE0_WORKSPACE_ASSURANCE" |
| <a id="s-5908522b13"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-635d6cb9ed"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_WORKSPACE_ASSURANCE](../../../evidence/sources.md#src-26ac73d782) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get('STOVE0_WORKSPACE_ASSURANCE', 'encrypted')` |

### Machine authority

- `/external_contract/configuration_environment/125`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68f8085ea9f59d7a81a32481c069f03665d599acfba61f7aabe7bee9c79b577f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_WORKSPACE_ASSURANCE",
  "name": "STOVE0_WORKSPACE_ASSURANCE",
  "owner": "stove0-server"
}
```
