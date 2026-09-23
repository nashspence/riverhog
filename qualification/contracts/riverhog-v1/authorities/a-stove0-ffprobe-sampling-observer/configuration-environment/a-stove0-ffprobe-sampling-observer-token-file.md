# A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-token-file:9a60090d66 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bd353547c1"></a>

| Field | Value |
|---|---|
| <a id="s-99a088c360"></a>`consumers` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-82eb3d9be1"></a>`default_expressions` | `["unset"]` |
| <a id="s-ba76881dc9"></a>`id` | `"a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE"` |
| <a id="s-217d52381b"></a>`input_shape` | `"environment-string"` |
| <a id="s-4c310f1da8"></a>`name` | `"A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE"` |
| <a id="s-da6797fecf"></a>`owner` | `"a-stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-022d235ba7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-ffprobe-sampling-observer:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE](../../../evidence/sources/authorities.md#src-60857d42e2) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py::\_secret](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-ffprobe-sampling-observer` | [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py) | `os.getenv('A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/132`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf0eff77255906476931fc13787c6cd32f435977aeb3880b478270bd81e76764 -->

```json
{
  "consumers": [
    "a-stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE",
  "owner": "a-stove0-ffprobe-sampling-observer"
}
```

</details>
