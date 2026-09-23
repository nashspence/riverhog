# A_STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-host:02537ea22e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-369e6c5b50"></a>

| Field | Value |
|---|---|
| <a id="s-8820447a9b"></a>`consumers` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-9840fd3088"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-02bae8fe60"></a>`id` | `"a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST"` |
| <a id="s-f6dbd4d759"></a>`input_shape` | `"environment-string"` |
| <a id="s-db6f7ec6d4"></a>`name` | `"A_STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST"` |
| <a id="s-df0ee5554e"></a>`owner` | `"a-stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-03c191bffd"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-ffprobe-sampling-observer:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST](../../../evidence/sources/authorities.md#src-3f58a4fec4) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py::\_parser](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-ffprobe-sampling-observer` | [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py) | `os.getenv('A_STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/127`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1eae41fb2a360b24aefe18def783ec09a56824ee73cf89ceb66a7a39d249a113 -->

```json
{
  "consumers": [
    "a-stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST",
  "input_shape": "environment-string",
  "name": "A_STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST",
  "owner": "a-stove0-ffprobe-sampling-observer"
}
```

</details>
