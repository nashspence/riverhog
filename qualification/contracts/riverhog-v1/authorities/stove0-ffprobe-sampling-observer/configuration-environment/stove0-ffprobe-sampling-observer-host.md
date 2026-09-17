# STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-host:18fe71614d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-94c51ac809"></a>

| Field | Value |
|---|---|
| <a id="s-a660901212"></a>`consumers` | `["stove0-ffprobe-sampling-observer"]` |
| <a id="s-3cd6dfb5ef"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-1b02ca218c"></a>`id` | `"stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST"` |
| <a id="s-7e702f68e0"></a>`input_shape` | `"environment-string"` |
| <a id="s-7f85e987aa"></a>`name` | `"STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST"` |
| <a id="s-43642d922f"></a>`owner` | `"stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-4cf1a9ed31"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST](../../../evidence/sources/authorities.md#src-1ede0d200b) — [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py::\_parser](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py) | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/157`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2fb375e44b14fc529c07684f14b0e47804572bc98b7ff2b3e95be57004fa6bdd -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST",
  "owner": "stove0-ffprobe-sampling-observer"
}
```

</details>
