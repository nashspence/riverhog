# A_REVIEW0_RCLONE_TARGET_DESTINATION_IDENTITY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-destination-identity:90741ea2ac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c4b672867c"></a>

| Field | Value |
|---|---|
| <a id="s-396c43f461"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-d2f643cba8"></a>`default_expressions` | `["''"]` |
| <a id="s-3f3f65b51c"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_DESTINATION_IDENTITY"` |
| <a id="s-c9113b6c41"></a>`input_shape` | `"environment-string"` |
| <a id="s-019de684d7"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_DESTINATION_IDENTITY"` |
| <a id="s-cd554621c2"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-a2756fc178"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_DESTINATION_IDENTITY](../../../evidence/sources/authorities.md#src-997697e84e) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_effect\_destination](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_DESTINATION_IDENTITY', '')` |

### Machine authority

- `/external_contract/configuration_environment/31`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e7dc5f34f24f1f45540f859e3c093a89802eb02e0a6101f69f6aaa7c56ea831 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_DESTINATION_IDENTITY",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_DESTINATION_IDENTITY",
  "owner": "a-review0-rclone-target"
}
```

</details>
