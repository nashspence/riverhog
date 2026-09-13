# STOVE0_OPUS_TARGET_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-source-revision:33ae2f5aa0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-b0c1830930) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4d9e9baa2d"></a>
| Field | Shape |
|---|---|
| <a id="s-ac1870ac7e"></a>`consumers` | ["stove0-opus-target"] |
| <a id="s-dbeac2fd91"></a>`default_expressions` | ["'unknown'"] |
| <a id="s-88ec9d4401"></a>`id` | "stove0-opus-target:environment:STOVE0_OPUS_TARGET_SOURCE_REVISION" |
| <a id="s-088ce46815"></a>`input_shape` | "environment-string" |
| <a id="s-4eb4f92e09"></a>`name` | "STOVE0_OPUS_TARGET_SOURCE_REVISION" |
| <a id="s-090693892d"></a>`owner` | "stove0-opus-target" |

## Governing policies

- <a id="pa-0dc9008723"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_SOURCE_REVISION](../../../evidence/sources.md#src-2bdb4a9e8f) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv(f'{prefix}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/194`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eaa0fef7587eeda5f67de1ee721d9e9dc1aa7079ecb6e8e355827b6fd9250e6b -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_SOURCE_REVISION",
  "owner": "stove0-opus-target"
}
```
