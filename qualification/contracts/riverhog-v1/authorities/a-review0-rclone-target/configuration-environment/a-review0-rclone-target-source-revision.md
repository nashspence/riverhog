# A_REVIEW0_RCLONE_TARGET_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-source-revision:5bbdc6511b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b200cddc47"></a>

| Field | Value |
|---|---|
| <a id="s-a64ea888cd"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-45ea5bbfc8"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-114bfa5509"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_SOURCE_REVISION"` |
| <a id="s-e3a22daa43"></a>`input_shape` | `"environment-string"` |
| <a id="s-5b43242159"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_SOURCE_REVISION"` |
| <a id="s-3435db812d"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-199e5db7eb"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_SOURCE_REVISION](../../../evidence/sources/authorities.md#src-1f8441209d) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::main](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/41`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 037b7a2f7be1215bba82e9e8748536519d07e8a32c1833ecfbdb0faa960a76c4 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_SOURCE_REVISION",
  "owner": "a-review0-rclone-target"
}
```

</details>
