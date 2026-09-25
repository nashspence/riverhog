# A_REVIEW0_RCLONE_TARGET_IMAGE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-image-id:cd7592034c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2d908ff8df"></a>

| Field | Value |
|---|---|
| <a id="s-c799298a70"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-539ad8b835"></a>`default_expressions` | `["''"]` |
| <a id="s-69dffd9a14"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_IMAGE_ID"` |
| <a id="s-f8a11ba5cc"></a>`input_shape` | `"environment-string"` |
| <a id="s-c530fa2f45"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_IMAGE_ID"` |
| <a id="s-c6f518f261"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-54f35ed79c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_IMAGE_ID](../../../evidence/sources/authorities.md#src-dd5eb278f8) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_image\_id](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_IMAGE_ID', '')` |

### Machine authority

- `/external_contract/configuration_environment/30`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dccd03ef13195312b3dd2d8c452b505423f14a7894fc4fc73788025df1590a3a -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_IMAGE_ID",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_IMAGE_ID",
  "owner": "a-review0-rclone-target"
}
```

</details>
