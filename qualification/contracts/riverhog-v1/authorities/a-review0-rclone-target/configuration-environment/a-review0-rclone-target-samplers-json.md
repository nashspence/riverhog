# A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-samplers-json:7aaf1f9e4c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-71ed3f559b"></a>

| Field | Value |
|---|---|
| <a id="s-93ede9eab6"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-5513911e29"></a>`default_expressions` | `["unset"]` |
| <a id="s-b5799a6efe"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON"` |
| <a id="s-12fb5098c2"></a>`input_shape` | `"environment-string"` |
| <a id="s-a3d7c530e4"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON"` |
| <a id="s-23d7da2e59"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-9a7357a7b9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON](../../../evidence/sources/authorities.md#src-e085983797) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_sampler\_registrations](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_SAMPLERS_JSON')` |

### Machine authority

- `/external_contract/configuration_environment/39`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56c625b38cd383f04e1bcecdd7c08658426978f7bfa860028a8591eff27e6d81 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON",
  "owner": "a-review0-rclone-target"
}
```

</details>
