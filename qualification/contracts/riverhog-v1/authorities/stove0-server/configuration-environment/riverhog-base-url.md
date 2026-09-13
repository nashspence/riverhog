# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:riverhog-base-url:b9fde6abda -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-12475197c9) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9b23dc5a6d"></a>
| Field | Shape |
|---|---|
| <a id="s-b36ae3a0c9"></a>`consumers` | ["stove0-server"] |
| <a id="s-1e4f606068"></a>`default_expressions` | ["''"] |
| <a id="s-6128334e2a"></a>`id` | "stove0-server:environment:RIVERHOG_BASE_URL" |
| <a id="s-e8b10c96db"></a>`input_shape` | "environment-string" |
| <a id="s-8c2675ed65"></a>`name` | "RIVERHOG_BASE_URL" |
| <a id="s-c9cdbffd9c"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-65380e19ac"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:RIVERHOG_BASE_URL](../../../evidence/sources.md#src-94aba7e378) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/225`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c27d2d5ece50268d1806c650e16d760dfbb3dbb42c22524bd59c0563fa5567f1 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:RIVERHOG_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BASE_URL",
  "owner": "stove0-server"
}
```
