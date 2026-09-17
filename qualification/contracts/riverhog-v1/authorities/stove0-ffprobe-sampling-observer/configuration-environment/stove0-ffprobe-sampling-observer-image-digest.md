# STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-image-digest:afa0abe25a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-702b6e89b0"></a>

| Field | Value |
|---|---|
| <a id="s-80c38a9bb0"></a>`consumers` | `["stove0-ffprobe-sampling-observer"]` |
| <a id="s-fcbdf40d24"></a>`default_expressions` | `["''"]` |
| <a id="s-fe4d544a2c"></a>`id` | `"stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST"` |
| <a id="s-e0043c7ac0"></a>`input_shape` | `"environment-string"` |
| <a id="s-3bcc5ed990"></a>`name` | `"STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST"` |
| <a id="s-c44d6a61f5"></a>`owner` | `"stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-24ce1da26d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-64a7dc58ec) — [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py::\_image\_digest](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py) | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/158`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22e145409b2e8cb62626f99e05c0db045b06fb1f33c64850c53dd7ae32703c83 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST",
  "owner": "stove0-ffprobe-sampling-observer"
}
```

</details>
