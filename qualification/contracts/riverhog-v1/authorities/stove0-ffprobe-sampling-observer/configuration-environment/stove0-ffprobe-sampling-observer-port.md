# STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-port:e0f2c5a859 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-872ceb948b"></a>
| Field | Shape |
|---|---|
| <a id="s-62bc68eec3"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-a72bf2915e"></a>`default_expressions` | ["'8080'"] |
| <a id="s-c849514bfd"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT" |
| <a id="s-0649d98f4f"></a>`input_shape` | "environment-string" |
| <a id="s-c61b2a9ee7"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT" |
| <a id="s-310d14fa51"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-6cc3645573"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT](../../../evidence/sources.md#src-b6e1a506b4) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/159`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14113d8ba9ff8317cc92a7055675a8193ef49a62c97e8862dd960d5832484c2f -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
