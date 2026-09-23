# A_STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-source-revision:25fb3b9aa6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9eab401a70"></a>

| Field | Value |
|---|---|
| <a id="s-218e41012f"></a>`consumers` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-f08ae4f8ca"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-64dc21d5bb"></a>`id` | `"a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION"` |
| <a id="s-76c70f63c7"></a>`input_shape` | `"environment-string"` |
| <a id="s-7da90dc284"></a>`name` | `"A_STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION"` |
| <a id="s-7e0d15a543"></a>`owner` | `"a-stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-3c907c7f27"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-ffprobe-sampling-observer:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION](../../../evidence/sources/authorities.md#src-ecfaa40ca2) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py::main](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-ffprobe-sampling-observer` | [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py) | `os.getenv('A_STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/130`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc703f50bc29f9b470199b4e1848bf075ee0c43a960ebc0a6c409f7d0da72f3b -->

```json
{
  "consumers": [
    "a-stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "A_STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION",
  "owner": "a-stove0-ffprobe-sampling-observer"
}
```

</details>
