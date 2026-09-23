# A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-samplers-json-file:c7eef92106 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c26bd33aa8"></a>

| Field | Value |
|---|---|
| <a id="s-cbfe80a062"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-8d23212c1a"></a>`default_expressions` | `["unset"]` |
| <a id="s-a3f3fe3044"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON_FILE"` |
| <a id="s-7d81361521"></a>`input_shape` | `"environment-string"` |
| <a id="s-501ec97f37"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON_FILE"` |
| <a id="s-06c79d612b"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-ff47677886"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON_FILE](../../../evidence/sources/authorities.md#src-cf746a1a10) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_sampler\_registrations](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_SAMPLERS_JSON_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/40`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1ff38fa942e612da1b6c12061a416cdb46c445b859d9fc6770cefc69ad5ca3a -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON_FILE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_SAMPLERS_JSON_FILE",
  "owner": "a-review0-rclone-target"
}
```

</details>
