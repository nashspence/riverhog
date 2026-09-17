# STOVE0_OPUS_TARGET_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-host:911a7c70d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-fb2f551b7b"></a>

| Field | Value |
|---|---|
| <a id="s-0b33eff101"></a>`consumers` | `["stove0-opus-target"]` |
| <a id="s-db5b575119"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-a91de35fd0"></a>`id` | `"stove0-opus-target:environment:STOVE0_OPUS_TARGET_HOST"` |
| <a id="s-af63f043ee"></a>`input_shape` | `"environment-string"` |
| <a id="s-78304b2742"></a>`name` | `"STOVE0_OPUS_TARGET_HOST"` |
| <a id="s-d27659d4cc"></a>`owner` | `"stove0-opus-target"` |

## Governing policies

- <a id="pa-b7e9fb6355"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_HOST](../../../evidence/sources.md#src-8aa1fc04d2) — [reference/stove0/targets/opus/target/src/stove0\_opus\_target/app.py::\_parser](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | [reference/stove0/targets/opus/target/src/stove0\_opus\_target/app.py](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/app.py) | `os.getenv(f'{prefix}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/191`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5303848b3dd59d8810488abdf092f924f3ed4e3b3dd233f3a39165b2e19ce5e2 -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_HOST",
  "owner": "stove0-opus-target"
}
```

</details>
