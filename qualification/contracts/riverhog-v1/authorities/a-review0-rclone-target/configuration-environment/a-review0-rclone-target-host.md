# A_REVIEW0_RCLONE_TARGET_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-host:4b23a3cf8c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-a9fb2eff34"></a>

| Field | Value |
|---|---|
| <a id="s-c00038c38f"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-5600fa3893"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-c9b454920d"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_HOST"` |
| <a id="s-bca3c308a4"></a>`input_shape` | `"environment-string"` |
| <a id="s-caff9576d7"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_HOST"` |
| <a id="s-69d21c7a53"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-2be3de6109"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_HOST](../../../evidence/sources/authorities.md#src-f64e71c085) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_parser](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/32`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f1f2f49e5aec556d7098c0d99a110b0ae7d8f894a6f48652d0a1dbea5f4d818 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_HOST",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_HOST",
  "owner": "a-review0-rclone-target"
}
```

</details>
