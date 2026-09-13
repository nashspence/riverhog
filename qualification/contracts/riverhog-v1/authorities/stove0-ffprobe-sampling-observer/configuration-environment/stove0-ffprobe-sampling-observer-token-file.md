# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-token-file:349f609026 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-29be11acec"></a>
| Field | Shape |
|---|---|
| <a id="s-9e151f5d6f"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-bf93b03aae"></a>`default_expressions` | ["unset"] |
| <a id="s-453e030848"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE" |
| <a id="s-0676e4ac00"></a>`input_shape` | "environment-string" |
| <a id="s-7b3cc3529a"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE" |
| <a id="s-254f42a6bc"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-89c59bc2ef"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE](../../../evidence/sources.md#src-476999fdb4) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/162`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac0f3c63c3c9f7f27e364e8577e5f169652083e65189040453e8dd340b18b2e5 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
