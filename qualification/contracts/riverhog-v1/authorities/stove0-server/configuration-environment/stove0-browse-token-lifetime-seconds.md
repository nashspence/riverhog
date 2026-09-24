# STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-browse-token-lifetime-seconds:e5435e5bb0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1e11f442b1"></a>

| Field | Value |
|---|---|
| <a id="s-1da413efaf"></a>`consumers` | `["stove0-server"]` |
| <a id="s-0d19b67e16"></a>`default_expressions` | `["str(default)"]` |
| <a id="s-ffe3cd761b"></a>`id` | `"stove0-server:environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"` |
| <a id="s-a9dc398834"></a>`input_shape` | `"environment-string"` |
| <a id="s-6c7bb367ae"></a>`name` | `"STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"` |
| <a id="s-7f6a641ee0"></a>`owner` | `"stove0-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](#s-1e11f442b1) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0f4812c6b7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-21733b4c74"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../../../evidence/sources/authorities.md#src-8e79bc10a5) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_integer](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/230`

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
