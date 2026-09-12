# STOVE0_ADMISSIONS_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-admissions-path:04149e49eb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-d2342f695d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-63f2a8f8ec"></a>
| Field | Shape |
|---|---|
| <a id="s-bf6277d5e7"></a>`classification` | "identity" |
| <a id="s-7635dc7bce"></a>`consumers` | ["stove0-server"] |
| <a id="s-0d7985a106"></a>`disposition` | "contractual" |
| <a id="s-5ba55aed0e"></a>`id` | "stove0-server:environment:STOVE0_ADMISSIONS_PATH" |
| <a id="s-0b8c0a3aae"></a>`name` | "STOVE0_ADMISSIONS_PATH" |
| <a id="s-26fd49b762"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-95a2b8017b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_ADMISSIONS_PATH](../../../evidence/sources.md#src-3e52273877) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/27/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get('STOVE0_ADMISSIONS_PATH', '')` |

### Machine authority

- `/external_contract/configuration_environment/109`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b786d8a4011a49e5d9f9ea46af29d1b03d414275af944fd8039ae93df72f607e -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_ADMISSIONS_PATH",
  "name": "STOVE0_ADMISSIONS_PATH",
  "owner": "stove0-server"
}
```
