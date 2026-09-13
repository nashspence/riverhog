# STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-workspace:cc5e764b52 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-d7a496f867) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e9ff2ea405"></a>
| Field | Shape |
|---|---|
| <a id="s-b6356ce043"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-8c154d027c"></a>`default_expressions` | ["'/run/stove0-ffprobe-sampling-observer'"] |
| <a id="s-da0d41e3d2"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE" |
| <a id="s-36e9d60abf"></a>`input_shape` | "environment-string" |
| <a id="s-fd9ccf6d49"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE" |
| <a id="s-29fdfb5587"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-4a4b440dbd"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE](../../../evidence/sources.md#src-c36f19fb69) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE', '/run/stove0-ffprobe-sampling-observer')` |

### Machine authority

- `/external_contract/configuration_environment/163`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d9de19dbb84e7b914f60ff5f939f276c04d94fcec490eb046f56381e4f5b0ed -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'/run/stove0-ffprobe-sampling-observer'"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
