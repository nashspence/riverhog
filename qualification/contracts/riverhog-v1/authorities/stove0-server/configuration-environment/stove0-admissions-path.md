# STOVE0_ADMISSIONS_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-admissions-path:a5d5ed1253 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-fb95996087"></a>

| Field | Value |
|---|---|
| <a id="s-0cfaaf5cc7"></a>`consumers` | `["stove0-server"]` |
| <a id="s-6d7f32c41a"></a>`default_expressions` | `["''"]` |
| <a id="s-335c2b4e87"></a>`id` | `"stove0-server:environment:STOVE0_ADMISSIONS_PATH"` |
| <a id="s-35470fd7f7"></a>`input_shape` | `"environment-string"` |
| <a id="s-081e60778b"></a>`name` | `"STOVE0_ADMISSIONS_PATH"` |
| <a id="s-04c95d4c9b"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-d4014f67e8"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_ADMISSIONS_PATH](../../../evidence/sources/authorities.md#src-3e52273877) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_admissions](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get('STOVE0_ADMISSIONS_PATH', '')` |

### Machine authority

- `/external_contract/configuration_environment/227`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acb1d11e5c92f10fd750358fd57a2c1cf682d5407353ef7bea6328bb129544dd -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_ADMISSIONS_PATH",
  "input_shape": "environment-string",
  "name": "STOVE0_ADMISSIONS_PATH",
  "owner": "stove0-server"
}
```

</details>
