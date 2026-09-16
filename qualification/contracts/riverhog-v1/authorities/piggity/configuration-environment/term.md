# TERM

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:term:91e2146258 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8f9926b885"></a>

| Field | Value |
|---|---|
| <a id="s-9172f4f7f0"></a>`consumers` | `["piggity"]` |
| <a id="s-0f0e40311d"></a>`default_expressions` | `["unset"]` |
| <a id="s-4e688baa7f"></a>`id` | `"piggity:environment:TERM"` |
| <a id="s-25ca4126b5"></a>`input_shape` | `"environment-string"` |
| <a id="s-1fd2f143d5"></a>`name` | `"TERM"` |
| <a id="s-42b278dcfa"></a>`owner` | `"piggity"` |

## Governing policies

- <a id="pa-47846cc162"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:piggity:TERM](../../../evidence/sources.md#src-6ee2b49508) — `reference/riverhog/applications/piggity/src/piggity/cli_support.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/cli_support.py` | `os.getenv('TERM')` |

### Machine authority

- `/external_contract/configuration_environment/11`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0dff8b8cea82475d08ec6752f37663035dc1cd5272b5d094a4c341dfcb5185a -->

```json
{
  "consumers": [
    "piggity"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "piggity:environment:TERM",
  "input_shape": "environment-string",
  "name": "TERM",
  "owner": "piggity"
}
```

</details>
