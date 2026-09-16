# STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-browse-token-lifetime-seconds:79227c37e7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f1fbb5a5d4"></a>

| Field | Value |
|---|---|
| <a id="s-3783a83c9c"></a>`consumers` | `["stove0-server"]` |
| <a id="s-55628ac85d"></a>`default_expressions` | `["str(default)"]` |
| <a id="s-bf201c7363"></a>`id` | `"stove0-server:environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"` |
| <a id="s-66ede93aca"></a>`input_shape` | `"environment-string"` |
| <a id="s-54e94dbbfa"></a>`name` | `"STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"` |
| <a id="s-c25d212d2e"></a>`owner` | `"stove0-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](#s-f1fbb5a5d4) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-011978f66d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-cf31f5861f"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../../../evidence/sources.md#src-8e79bc10a5) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/231`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5bc539348f466360be1e2d81cdac7c65c8e7b34d74db4775b27f981719fc9955 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "str(default)"
  ],
  "id": "stove0-server:environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS",
  "owner": "stove0-server"
}
```

</details>
