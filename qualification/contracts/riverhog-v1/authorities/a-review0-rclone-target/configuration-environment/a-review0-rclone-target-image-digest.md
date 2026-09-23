# A_REVIEW0_RCLONE_TARGET_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-image-digest:5ada631aad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f77147b1c3"></a>

| Field | Value |
|---|---|
| <a id="s-4504ac9a24"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-dd23c689a9"></a>`default_expressions` | `["''"]` |
| <a id="s-d6c9861ab7"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_IMAGE_DIGEST"` |
| <a id="s-327d2c8e3c"></a>`input_shape` | `"environment-string"` |
| <a id="s-8af67c498b"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_IMAGE_DIGEST"` |
| <a id="s-e1481d42fc"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-030b850d9f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-2a0ed56aaa) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_image\_digest](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/33`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e48d0b6b38bd40256c3803a95c8fbfe07fd953b5ee6a4ae9f97daa45cd2a92c4 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_IMAGE_DIGEST",
  "owner": "a-review0-rclone-target"
}
```

</details>
