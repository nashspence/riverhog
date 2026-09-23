# A_STOVE0_OPUS_TARGET_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-image-digest:6deb0c06b5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5d16c3676e"></a>

| Field | Value |
|---|---|
| <a id="s-a4f6afb693"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-3a22260cfc"></a>`default_expressions` | `["''"]` |
| <a id="s-b53af91022"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_IMAGE_DIGEST"` |
| <a id="s-5fe728f6a8"></a>`input_shape` | `"environment-string"` |
| <a id="s-93dd6b430c"></a>`name` | `"A_STOVE0_OPUS_TARGET_IMAGE_DIGEST"` |
| <a id="s-36acc9190a"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-76fe48e8bd"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-6b954868f1) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::\_image\_digest](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/146`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67f9aa6204ca167d0f5466fbe428bc2b407d5fd05da403bade06a1a4fe98cdd3 -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_IMAGE_DIGEST",
  "owner": "a-stove0-opus-target"
}
```

</details>
