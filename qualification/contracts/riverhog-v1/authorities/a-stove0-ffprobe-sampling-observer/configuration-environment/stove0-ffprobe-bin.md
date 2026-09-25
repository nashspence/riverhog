# STOVE0_FFPROBE_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-ffprobe-sampling-observer:stove0-ffprobe-bin:0a207b9e77 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3efbdd0d50"></a>

| Field | Value |
|---|---|
| <a id="s-e5a8456801"></a>`consumers` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-7044fe0ae8"></a>`default_expressions` | `["'ffprobe'"]` |
| <a id="s-c4e21ab4c8"></a>`id` | `"a-stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_BIN"` |
| <a id="s-ee31f3135d"></a>`input_shape` | `"environment-string"` |
| <a id="s-8bf9d9fca2"></a>`name` | `"STOVE0_FFPROBE_BIN"` |
| <a id="s-33c76d2bc9"></a>`owner` | `"a-stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-04ef071d85"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_BIN](../../../evidence/sources/authorities.md#src-0898d04d6d) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py::main](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-ffprobe-sampling-observer` | [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py) | `os.getenv('STOVE0_FFPROBE_BIN', 'ffprobe')` |

### Machine authority

- `/external_contract/configuration_environment/78`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6732bcd7425661a054bc05eaf6bd16e3042a0ee31be5cc54f5c1bbf374f45785 -->

```json
{
  "consumers": [
    "a-stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'ffprobe'"
  ],
  "id": "a-stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_BIN",
  "owner": "a-stove0-ffprobe-sampling-observer"
}
```

</details>
