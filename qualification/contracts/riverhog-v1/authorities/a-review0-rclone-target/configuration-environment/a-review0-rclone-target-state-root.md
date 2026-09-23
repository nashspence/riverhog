# A_REVIEW0_RCLONE_TARGET_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-state-root:d62d7c5813 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ba8bf00da0"></a>

| Field | Value |
|---|---|
| <a id="s-fefed5a14e"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-9f9519b503"></a>`default_expressions` | `["'/var/lib/a-review0-rclone-target'"]` |
| <a id="s-6e9078eeef"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_STATE_ROOT"` |
| <a id="s-b017fa7a37"></a>`input_shape` | `"environment-string"` |
| <a id="s-5c224fe460"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_STATE_ROOT"` |
| <a id="s-316a058fc1"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-a2f7e9d65d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_STATE_ROOT](../../../evidence/sources/authorities.md#src-dc314e3ad4) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::main](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_STATE_ROOT', '/var/lib/a-review0-rclone-target')` |

### Machine authority

- `/external_contract/configuration_environment/42`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a99483a391026bcef8f5ce625da6390c8f4bb9d992ee6b59aa9299ed2fbfa6b -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "'/var/lib/a-review0-rclone-target'"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_STATE_ROOT",
  "owner": "a-review0-rclone-target"
}
```

</details>
