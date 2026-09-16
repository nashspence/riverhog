# XDG_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-provenance:xdg-state-home:6ffca58246 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1545d1acc2"></a>

| Field | Value |
|---|---|
| <a id="s-5a43b64463"></a>`consumers` | `["riverhog-provenance"]` |
| <a id="s-be1388564a"></a>`default_expressions` | `["unset"]` |
| <a id="s-84bf2d47b0"></a>`id` | `"riverhog-provenance:environment:XDG_STATE_HOME"` |
| <a id="s-74d37a60a9"></a>`input_shape` | `"environment-string"` |
| <a id="s-e838cafed4"></a>`name` | `"XDG_STATE_HOME"` |
| <a id="s-ee0809963a"></a>`owner` | `"riverhog-provenance"` |

## Governing policies

- <a id="pa-982044df1f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-provenance:XDG_STATE_HOME](../../../evidence/sources.md#src-fb8d65585b) — `packages/riverhog-provenance/src/riverhog_provenance/identity.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-provenance` | `packages/riverhog-provenance/src/riverhog_provenance/identity.py` | `os.getenv('XDG_STATE_HOME')` |

### Machine authority

- `/external_contract/configuration_environment/34`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44e545ff62be823f0c16cf187dc89d2b704134b09f3ed24fd325bf3bba5b4388 -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-provenance:environment:XDG_STATE_HOME",
  "input_shape": "environment-string",
  "name": "XDG_STATE_HOME",
  "owner": "riverhog-provenance"
}
```

</details>
