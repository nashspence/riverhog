# A_REVIEW0_RCLONE_TARGET_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-token-file:cf3de5f48e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ee975e99ad"></a>

| Field | Value |
|---|---|
| <a id="s-ca55d6dc58"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-306a5ba67b"></a>`default_expressions` | `["unset"]` |
| <a id="s-777167e457"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_TOKEN_FILE"` |
| <a id="s-faf3e3ddf2"></a>`input_shape` | `"environment-string"` |
| <a id="s-1a8ef76883"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_TOKEN_FILE"` |
| <a id="s-1095e66092"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-e4ba730e83"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_TOKEN_FILE](../../../evidence/sources/authorities.md#src-cc43a8e7c2) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_secret](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/44`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94126be99216bc95f95e03cc6830b2ffe445528cf834592c343c4caf2f7f5487 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_TOKEN_FILE",
  "owner": "a-review0-rclone-target"
}
```

</details>
