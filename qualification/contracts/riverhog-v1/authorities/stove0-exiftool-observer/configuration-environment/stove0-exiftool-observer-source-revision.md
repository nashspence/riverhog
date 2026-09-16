# STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-source-revision:4c939832aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1b4b47f8eb"></a>

| Field | Value |
|---|---|
| <a id="s-fc4c1b6f7b"></a>`consumers` | `["stove0-exiftool-observer"]` |
| <a id="s-ad70defaf4"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-142147407b"></a>`id` | `"stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION"` |
| <a id="s-63ba3637de"></a>`input_shape` | `"environment-string"` |
| <a id="s-56e871d5e4"></a>`name` | `"STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION"` |
| <a id="s-230d2f78c0"></a>`owner` | `"stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-ade61ad4a2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION](../../../evidence/sources.md#src-29eef6aa4c) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/152`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b4cfc7c34ba993e5f3d9c09a184103939c4d9e057459c8041ab5c30c27176fb -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION",
  "owner": "stove0-exiftool-observer"
}
```

</details>
