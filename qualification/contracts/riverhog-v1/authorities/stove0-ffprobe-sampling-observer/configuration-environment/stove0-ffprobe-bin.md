# STOVE0_FFPROBE_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-bin:c0a105514b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ebe5f6e564"></a>

| Field | Value |
|---|---|
| <a id="s-bbc93bda10"></a>`consumers` | `["stove0-ffprobe-sampling-observer"]` |
| <a id="s-280060eac6"></a>`default_expressions` | `["'ffprobe'"]` |
| <a id="s-a700ea2fa0"></a>`id` | `"stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_BIN"` |
| <a id="s-3c3b66b4ca"></a>`input_shape` | `"environment-string"` |
| <a id="s-4e5ff21edf"></a>`name` | `"STOVE0_FFPROBE_BIN"` |
| <a id="s-2def1b5bdc"></a>`owner` | `"stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-141bed0ce4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_BIN](../../../evidence/sources.md#src-2e8c92027a) — [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py::main](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py) | `os.getenv('STOVE0_FFPROBE_BIN', 'ffprobe')` |

### Machine authority

- `/external_contract/configuration_environment/156`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f60df642ce4471f961ea165ffdd4f92aed753b8295fb83456083e67bf4cfd36 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'ffprobe'"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_BIN",
  "owner": "stove0-ffprobe-sampling-observer"
}
```

</details>
