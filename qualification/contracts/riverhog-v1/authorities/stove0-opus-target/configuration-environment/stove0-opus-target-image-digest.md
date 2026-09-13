# STOVE0_OPUS_TARGET_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-image-digest:0f9e1f3f93 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-787fd4b453"></a>
| Field | Shape |
|---|---|
| <a id="s-5f53685028"></a>`consumers` | ["stove0-opus-target"] |
| <a id="s-9f8a9d0439"></a>`default_expressions` | ["''"] |
| <a id="s-8cd9a2b240"></a>`id` | "stove0-opus-target:environment:STOVE0_OPUS_TARGET_IMAGE_DIGEST" |
| <a id="s-46621afb01"></a>`input_shape` | "environment-string" |
| <a id="s-18c3d1d37b"></a>`name` | "STOVE0_OPUS_TARGET_IMAGE_DIGEST" |
| <a id="s-7047319589"></a>`owner` | "stove0-opus-target" |

## Governing policies

- <a id="pa-f7e39d1067"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_IMAGE_DIGEST](../../../evidence/sources.md#src-284da96ce7) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv(f'{prefix}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/192`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e37a482f03567d25cf347b1a02a0d1c10b0344434312cfae063b0cd945fa9172 -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_IMAGE_DIGEST",
  "owner": "stove0-opus-target"
}
```
