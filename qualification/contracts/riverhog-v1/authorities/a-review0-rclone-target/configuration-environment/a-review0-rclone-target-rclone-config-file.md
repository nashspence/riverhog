# A_REVIEW0_RCLONE_TARGET_RCLONE_CONFIG_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-rclone-config-file:d72ac697cb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1c3c820c2c"></a>

| Field | Value |
|---|---|
| <a id="s-aca3251563"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-210c2b46d5"></a>`default_expressions` | `["''"]` |
| <a id="s-0463c6ecc3"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_RCLONE_CONFIG_FILE"` |
| <a id="s-36a54fae1a"></a>`input_shape` | `"environment-string"` |
| <a id="s-5945522bb9"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_RCLONE_CONFIG_FILE"` |
| <a id="s-8beef43a66"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-4b0cdd3189"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_RCLONE_CONFIG_FILE](../../../evidence/sources/authorities.md#src-fd31cda2f1) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_effect\_destination](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_RCLONE_CONFIG_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/36`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f979f10e6d7fb697e12b1b4480b01b44d63ee76eccf7fb1847e07f831cd786a -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_RCLONE_CONFIG_FILE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_RCLONE_CONFIG_FILE",
  "owner": "a-review0-rclone-target"
}
```

</details>
