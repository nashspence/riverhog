# STOVE0_FFPROBE_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-ffprobe-sampling-observer:stove0-ffprobe-bin:fa1d1446e5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-830a33f58f"></a>

| Field | Value |
|---|---|
| <a id="s-33dee50a09"></a>`consumers` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-d60d0efc38"></a>`default_expressions` | `["'ffprobe'"]` |
| <a id="s-1134d2a2b0"></a>`id` | `"a-stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_BIN"` |
| <a id="s-cf5d831d6b"></a>`input_shape` | `"environment-string"` |
| <a id="s-d6448f56b3"></a>`name` | `"STOVE0_FFPROBE_BIN"` |
| <a id="s-faa4c8819d"></a>`owner` | `"a-stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-136ee5cd4e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/134`

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
