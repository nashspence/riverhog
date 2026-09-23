# A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-rclone-timeout-seconds:a57a5a8cc7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c6af80cc8d"></a>

| Field | Value |
|---|---|
| <a id="s-dc771f99e6"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-119b2b8dfe"></a>`default_expressions` | `["'86400'"]` |
| <a id="s-a725fad84c"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS"` |
| <a id="s-9285c08ab7"></a>`input_shape` | `"environment-string"` |
| <a id="s-3869830d71"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS"` |
| <a id="s-ff15d2537d"></a>`owner` | `"a-review0-rclone-target"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS"; consumers=["a-review0-rclone-target"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS](#s-c6af80cc8d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e3e164b09a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-d8dc064c0f"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-73df25832c) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_effect\_destination](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_RCLONE_TIMEOUT_SECONDS', '86400')` |

### Machine authority

- `/external_contract/configuration_environment/38`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 65804d76c33b344bbaa0f4a89bdfe440d02e530fb939d409558f885d406e1c99 -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "'86400'"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_RCLONE_TIMEOUT_SECONDS",
  "owner": "a-review0-rclone-target"
}
```

</details>
