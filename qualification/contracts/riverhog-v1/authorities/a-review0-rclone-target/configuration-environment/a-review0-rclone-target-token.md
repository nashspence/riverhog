# A_REVIEW0_RCLONE_TARGET_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-token:368e682fd4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c80b060880"></a>

| Field | Value |
|---|---|
| <a id="s-fe8922d859"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-63325e3b83"></a>`default_expressions` | `["unset"]` |
| <a id="s-8546fde067"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_TOKEN"` |
| <a id="s-2ae46bdb0a"></a>`input_shape` | `"environment-string"` |
| <a id="s-e9729640af"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_TOKEN"` |
| <a id="s-09d7007204"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-20c498b9bf"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_TOKEN](../../../evidence/sources/authorities.md#src-173edf1574) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_secret](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py); [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::main](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_TOKEN')` |
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.environ.pop(f'{PREFIX}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/43`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8d7a9e5a317749253348faeb8d00cb14d748a359b55bb226787ab5205816c8a -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_TOKEN",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_TOKEN",
  "owner": "a-review0-rclone-target"
}
```

</details>
