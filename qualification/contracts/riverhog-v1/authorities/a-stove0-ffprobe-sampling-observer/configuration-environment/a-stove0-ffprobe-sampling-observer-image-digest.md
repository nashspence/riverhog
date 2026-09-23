# A_STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-image-digest:649e514031 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2c0c24555b"></a>

| Field | Value |
|---|---|
| <a id="s-388219734c"></a>`consumers` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-0cf6ba5e8e"></a>`default_expressions` | `["''"]` |
| <a id="s-aced32925f"></a>`id` | `"a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST"` |
| <a id="s-9209aa02f6"></a>`input_shape` | `"environment-string"` |
| <a id="s-67fd9ad639"></a>`name` | `"A_STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST"` |
| <a id="s-7ab9e38310"></a>`owner` | `"a-stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-461647fde6"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-ffprobe-sampling-observer:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-a222d87940) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py::\_image\_digest](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-ffprobe-sampling-observer` | [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py) | `os.getenv('A_STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/128`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcc504523a02624ec551ff7be6d14528888bcafd869db571d2ae9a4f521907c4 -->

```json
{
  "consumers": [
    "a-stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "A_STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST",
  "owner": "a-stove0-ffprobe-sampling-observer"
}
```

</details>
