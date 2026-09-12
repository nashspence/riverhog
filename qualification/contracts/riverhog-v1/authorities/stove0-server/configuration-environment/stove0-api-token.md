# STOVE0_API_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-api-token:3f97a38334 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-182b18fbb1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6cd1741870"></a>
| Field | Shape |
|---|---|
| <a id="s-162b0c277b"></a>`classification` | "credential" |
| <a id="s-f89c1c3048"></a>`consumers` | ["stove0-server"] |
| <a id="s-76707167a2"></a>`disposition` | "contractual" |
| <a id="s-ee9ed3533d"></a>`id` | "stove0-server:environment:STOVE0_API_TOKEN" |
| <a id="s-7027ddbe71"></a>`name` | "STOVE0_API_TOKEN" |
| <a id="s-5b4c4db475"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-4e45ba6ae8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_API_TOKEN](../../../evidence/sources.md#src-b54731a17b) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/26/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_secret(values, 'STOVE0_API_TOKEN', required=False)` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_secret(values, 'STOVE0_API_TOKEN', required=True)` |

### Machine authority

- `/external_contract/configuration_environment/110`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56a9907348886b7705d1942878e54da9d2f82602a76bf3465ee2754f3a8975e5 -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_API_TOKEN",
  "name": "STOVE0_API_TOKEN",
  "owner": "stove0-server"
}
```
