# STOVE0_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-browse-token-signing-key:7f21447c14 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-12475197c9) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4100c93200"></a>
| Field | Shape |
|---|---|
| <a id="s-23f3e07db6"></a>`consumers` | ["stove0-server"] |
| <a id="s-1e00eaa47b"></a>`default_expressions` | ["''"] |
| <a id="s-1c7f09fb76"></a>`id` | "stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY" |
| <a id="s-875aa8c93f"></a>`input_shape` | "environment-string" |
| <a id="s-c0627a9cb2"></a>`name` | "STOVE0_BROWSE_TOKEN_SIGNING_KEY" |
| <a id="s-ffaf3a09a2"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-7a0dfeb5e9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources.md#src-f552366814) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/232`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89c3493c390ee480a603e582cf34f29de33602a186b4d29808e60f7ae416689a -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY",
  "input_shape": "environment-string",
  "name": "STOVE0_BROWSE_TOKEN_SIGNING_KEY",
  "owner": "stove0-server"
}
```
