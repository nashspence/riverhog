# A_REVIEW0_RCLONE_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-workspace:1cbd5c7596 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6710649fb3"></a>

| Field | Value |
|---|---|
| <a id="s-fe5e426476"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-8068b7b57e"></a>`default_expressions` | `["'/run/review0'"]` |
| <a id="s-6bfdc5327e"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_WORKSPACE"` |
| <a id="s-62400e816f"></a>`input_shape` | `"environment-string"` |
| <a id="s-3a8258723c"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_WORKSPACE"` |
| <a id="s-373b12e6ec"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-5a290d51a8"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_WORKSPACE](../../../evidence/sources/authorities.md#src-2e10bb793e) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::main](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_WORKSPACE', '/run/review0')` |

### Machine authority

- `/external_contract/configuration_environment/35`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 243b6a9a5bf0e98e8cc1560e404038a254f4cacf277a7a53fc607ebc082021c1 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "'/run/review0'"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_WORKSPACE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_WORKSPACE",
  "owner": "a-review0-rclone-target"
}
```

</details>
