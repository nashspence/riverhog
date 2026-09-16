# STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-source-revision:9139663b98 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7d7e7cda43"></a>

| Field | Value |
|---|---|
| <a id="s-f4f2969b9d"></a>`consumers` | `["stove0-ffprobe-sampling-observer"]` |
| <a id="s-4c4c62b03d"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-1098cc2cad"></a>`id` | `"stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION"` |
| <a id="s-bf75070dbb"></a>`input_shape` | `"environment-string"` |
| <a id="s-5d05b612f9"></a>`name` | `"STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION"` |
| <a id="s-f23adf8fb1"></a>`owner` | `"stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-36426a96d3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION](../../../evidence/sources.md#src-776ca532af) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/160`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd19e0c727e7d0606da03347c09caa2b83ac6e7add45dafd19f0e50a78961ce5 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION",
  "owner": "stove0-ffprobe-sampling-observer"
}
```

</details>
