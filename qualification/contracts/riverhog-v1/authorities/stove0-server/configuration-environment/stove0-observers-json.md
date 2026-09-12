# STOVE0_OBSERVERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-observers-json:78051fee18 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-d2342f695d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7c4bc7f8a3"></a>
| Field | Shape |
|---|---|
| <a id="s-29c1685294"></a>`classification` | "identity" |
| <a id="s-de3bbf83d5"></a>`consumers` | ["stove0-server"] |
| <a id="s-939075b802"></a>`disposition` | "contractual" |
| <a id="s-dc7c1a2e94"></a>`id` | "stove0-server:environment:STOVE0_OBSERVERS_JSON" |
| <a id="s-033e682cb8"></a>`name` | "STOVE0_OBSERVERS_JSON" |
| <a id="s-609af60b93"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-1215e09b79"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_OBSERVERS_JSON](../../../evidence/sources.md#src-5e770d1e0f) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/27/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_registrations(values, 'STOVE0_OBSERVERS_JSON', semantic_validators=True)` |

### Machine authority

- `/external_contract/configuration_environment/116`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 588788b24473e2f9091051a704d2dcc9aa0134fc9ed8faf5aee8517538a16088 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_OBSERVERS_JSON",
  "name": "STOVE0_OBSERVERS_JSON",
  "owner": "stove0-server"
}
```
