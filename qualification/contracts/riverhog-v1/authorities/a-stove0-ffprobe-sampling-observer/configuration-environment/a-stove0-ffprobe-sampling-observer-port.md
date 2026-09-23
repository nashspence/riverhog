# A_STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-port:a1b76f9fd6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-37916ffbac"></a>

| Field | Value |
|---|---|
| <a id="s-d11e2a81c9"></a>`consumers` | `["a-stove0-ffprobe-sampling-observer"]` |
| <a id="s-4c55035830"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-f8ea685fed"></a>`id` | `"a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT"` |
| <a id="s-a104fdac21"></a>`input_shape` | `"environment-string"` |
| <a id="s-55951875b9"></a>`name` | `"A_STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT"` |
| <a id="s-b688a3006f"></a>`owner` | `"a-stove0-ffprobe-sampling-observer"` |

## Governing policies

- <a id="pa-7a10f89f99"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-ffprobe-sampling-observer:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT](../../../evidence/sources/authorities.md#src-fb36c69d69) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py::\_parser](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-ffprobe-sampling-observer` | [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py) | `os.getenv('A_STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/129`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f61113641a45556e5e758667eee5fd94365e333a51f5f86acb709e32f5e792c -->

```json
{
  "consumers": [
    "a-stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-stove0-ffprobe-sampling-observer:environment:A_STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT",
  "input_shape": "environment-string",
  "name": "A_STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT",
  "owner": "a-stove0-ffprobe-sampling-observer"
}
```

</details>
