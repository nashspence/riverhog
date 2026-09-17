# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-token:d2b9e6bec2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8f59324207"></a>

| Field | Value |
|---|---|
| <a id="s-5ce476d592"></a>`consumers` | `["stove0-ffprobe-sampling-observer"]` |
| <a id="s-3b4a852639"></a>`default_expressions` | `["unset"]` |
| <a id="s-8697d6dfab"></a>`id` | `"stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN"` |
| <a id="s-7e4f4054fb"></a>`input_shape` | `"environment-string"` |
| <a id="s-514286516a"></a>`name` | `"STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN"` |
| <a id="s-2e2d9517c4"></a>`owner` | `"stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-9d672c982a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN](../../../evidence/sources.md#src-42afc1d184) — [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py::\_secret](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py); [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py::main](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py) | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN')` |
| parser | `stove0-ffprobe-sampling-observer` | [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py) | `os.environ.pop('STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/161`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a43cc3f05d1c204c5396a9ed677562411959e8047995d01db7acd62b54951cfe -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN",
  "owner": "stove0-ffprobe-sampling-observer"
}
```

</details>
