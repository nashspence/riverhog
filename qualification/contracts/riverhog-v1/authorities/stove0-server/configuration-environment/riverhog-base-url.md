# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:riverhog-base-url:a702fa7b93 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d907531ff5"></a>

| Field | Value |
|---|---|
| <a id="s-651656bfc3"></a>`consumers` | `["stove0-server"]` |
| <a id="s-65a4f0d9de"></a>`default_expressions` | `["''"]` |
| <a id="s-3ab60f095f"></a>`id` | `"stove0-server:environment:RIVERHOG_BASE_URL"` |
| <a id="s-21cd035f9e"></a>`input_shape` | `"environment-string"` |
| <a id="s-53770aad28"></a>`name` | `"RIVERHOG_BASE_URL"` |
| <a id="s-eb7289352e"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-1a81851e13"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:RIVERHOG_BASE_URL](../../../evidence/sources/authorities.md#src-94aba7e378) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_required](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/224`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
