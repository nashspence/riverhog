# A_STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-token-file:026bc7d18e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0860e7403d"></a>

| Field | Value |
|---|---|
| <a id="s-9be6b52731"></a>`consumers` | `["a-stove0-nvenc-av1-opus-target"]` |
| <a id="s-0bd5eb779f"></a>`default_expressions` | `["unset"]` |
| <a id="s-c18f78a25e"></a>`id` | `"a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE"` |
| <a id="s-6d50564c65"></a>`input_shape` | `"environment-string"` |
| <a id="s-880697c149"></a>`name` | `"A_STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE"` |
| <a id="s-7d7836cab8"></a>`owner` | `"a-stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-50b7a808bd"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-nvenc-av1-opus-target:A_STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE](../../../evidence/sources/authorities.md#src-abf3d38658) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py::\_secret](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-nvenc-av1-opus-target` | [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/141`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b93ff39ed644c8e7d25a2f8fe9f5ef4fb4b400c4dfae0415a1f81be45cd91a5d -->

```json
{
  "consumers": [
    "a-stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE",
  "owner": "a-stove0-nvenc-av1-opus-target"
}
```

</details>
