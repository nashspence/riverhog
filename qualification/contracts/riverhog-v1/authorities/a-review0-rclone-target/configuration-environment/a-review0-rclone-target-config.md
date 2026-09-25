# A_REVIEW0_RCLONE_TARGET_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-config:1182b41104 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d7a4e6e0e8"></a>

| Field | Value |
|---|---|
| <a id="s-a5e7a20adb"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-d5c63849b4"></a>`default_expressions` | `["unset"]` |
| <a id="s-f2739e6aac"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_CONFIG"` |
| <a id="s-7791533715"></a>`input_shape` | `"environment-string"` |
| <a id="s-495c4fd74d"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_CONFIG"` |
| <a id="s-eba1397ffc"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-2967cfb33a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_CONFIG](../../../evidence/sources/authorities.md#src-7efe93c228) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_parser](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_CONFIG')` |

### Machine authority

- `/external_contract/configuration_environment/28`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0cbac2034151a35908db7b0f92a78f9dee13b6fc1003aec8bba12d7da052fa9 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_CONFIG",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_CONFIG",
  "owner": "a-review0-rclone-target"
}
```

</details>
