# STOVE0_TARGETS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-targets-json:929df5bfcb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-d2342f695d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-965179fc46"></a>
| Field | Shape |
|---|---|
| <a id="s-3fd2b2519d"></a>`classification` | "identity" |
| <a id="s-4433b3c434"></a>`consumers` | ["stove0-server"] |
| <a id="s-db3e539bc5"></a>`disposition` | "contractual" |
| <a id="s-efd9f29cdd"></a>`id` | "stove0-server:environment:STOVE0_TARGETS_JSON" |
| <a id="s-4dd5bfca84"></a>`name` | "STOVE0_TARGETS_JSON" |
| <a id="s-7d68affc90"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-7220a53d5a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_TARGETS_JSON](../../../evidence/sources.md#src-05b1eb365e) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/27/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_registrations(values, 'STOVE0_TARGETS_JSON')` |

### Machine authority

- `/external_contract/configuration_environment/120`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 255eae4f4c15163b9511a39867539a005140ca83fc1f5ba6b6734105ee5d7893 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_TARGETS_JSON",
  "name": "STOVE0_TARGETS_JSON",
  "owner": "stove0-server"
}
```
