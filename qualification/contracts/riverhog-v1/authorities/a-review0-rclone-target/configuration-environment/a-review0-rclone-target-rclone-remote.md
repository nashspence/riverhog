# A_REVIEW0_RCLONE_TARGET_RCLONE_REMOTE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-rclone-remote:05f07a97b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ee8d4c5bb2"></a>

| Field | Value |
|---|---|
| <a id="s-9cef198e9c"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-4e914a87ae"></a>`default_expressions` | `["''"]` |
| <a id="s-c87995acf3"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_RCLONE_REMOTE"` |
| <a id="s-1c5840c446"></a>`input_shape` | `"environment-string"` |
| <a id="s-199cea8e22"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_RCLONE_REMOTE"` |
| <a id="s-a35e48e93a"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-569dc6b86b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_RCLONE_REMOTE](../../../evidence/sources/authorities.md#src-6224b7ea0a) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_effect\_destination](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_RCLONE_REMOTE', '')` |

### Machine authority

- `/external_contract/configuration_environment/37`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4cf2bf9574e2e64e534219497451dbc8a7d7bf1568ba6d59ab03c72ce10f85a -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_RCLONE_REMOTE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_RCLONE_REMOTE",
  "owner": "a-review0-rclone-target"
}
```

</details>
