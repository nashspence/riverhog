# PIGGITY_PLAIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-plain:d594f3f0fd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2436bad7d4"></a>

| Field | Value |
|---|---|
| <a id="s-478f37a840"></a>`consumers` | `["piggity"]` |
| <a id="s-a9c79e933b"></a>`default_expressions` | `["''"]` |
| <a id="s-7ed31707ad"></a>`id` | `"piggity:environment:PIGGITY_PLAIN"` |
| <a id="s-d5950a2bc2"></a>`input_shape` | `"environment-string"` |
| <a id="s-7d2e712aed"></a>`name` | `"PIGGITY_PLAIN"` |
| <a id="s-cfbebc15f6"></a>`owner` | `"piggity"` |

## Governing policies

- <a id="pa-917430a0c1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:piggity:PIGGITY_PLAIN](../../../evidence/sources.md#src-88f78b70a7) — [reference/riverhog/applications/piggity/src/piggity/cli\_support.py::plain\_output\_requested](../../../../../../reference/riverhog/applications/piggity/src/piggity/cli_support.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `piggity` | [reference/riverhog/applications/piggity/src/piggity/cli\_support.py](../../../../../../reference/riverhog/applications/piggity/src/piggity/cli_support.py) | `os.getenv(setting, '')` |

### Machine authority

- `/external_contract/configuration_environment/7`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3230064d49748fd9ecc547131e00ee80938caeca5769b8340989072bd269fcb7 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "piggity:environment:PIGGITY_PLAIN",
  "input_shape": "environment-string",
  "name": "PIGGITY_PLAIN",
  "owner": "piggity"
}
```

</details>
