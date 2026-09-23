# A_REVIEW0_RCLONE_TARGET_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-rclone-target:a-review0-rclone-target-port:9aea30bdd8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1545d1acc2"></a>

| Field | Value |
|---|---|
| <a id="s-5a43b64463"></a>`consumers` | `["a-review0-rclone-target"]` |
| <a id="s-be1388564a"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-84bf2d47b0"></a>`id` | `"a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_PORT"` |
| <a id="s-74d37a60a9"></a>`input_shape` | `"environment-string"` |
| <a id="s-e838cafed4"></a>`name` | `"A_REVIEW0_RCLONE_TARGET_PORT"` |
| <a id="s-ee0809963a"></a>`owner` | `"a-review0-rclone-target"` |

## Governing policies

- <a id="pa-f1fe6a8868"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-rclone-target:A_REVIEW0_RCLONE_TARGET_PORT](../../../evidence/sources/authorities.md#src-ebb636acbd) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::\_parser](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-rclone-target` | [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py) | `os.getenv(f'{PREFIX}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/34`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7657892c7f6bfd62abe9b5ecc4b4a86bdaeb85c04b0a2363876611f686cb05d -->

```json
{
  "consumers": [
    "a-review0-rclone-target"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-review0-rclone-target:environment:A_REVIEW0_RCLONE_TARGET_PORT",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_RCLONE_TARGET_PORT",
  "owner": "a-review0-rclone-target"
}
```

</details>
