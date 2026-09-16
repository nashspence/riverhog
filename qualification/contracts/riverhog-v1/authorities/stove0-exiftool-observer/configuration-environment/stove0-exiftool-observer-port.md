# STOVE0_EXIFTOOL_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-port:a30bb3854d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ad8549f367"></a>

| Field | Value |
|---|---|
| <a id="s-5f84c44a9d"></a>`consumers` | `["stove0-exiftool-observer"]` |
| <a id="s-803c47b609"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-8186a9c49e"></a>`id` | `"stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_PORT"` |
| <a id="s-3cc579fe3f"></a>`input_shape` | `"environment-string"` |
| <a id="s-1b65f6d773"></a>`name` | `"STOVE0_EXIFTOOL_OBSERVER_PORT"` |
| <a id="s-d4381322eb"></a>`owner` | `"stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-f5db898246"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_PORT](../../../evidence/sources.md#src-ee07e99c6c) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/151`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be4eb16162b4fdff0ca84c29de4834b650270ae6bef7e4ee0dad14758fc8b55b -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_OBSERVER_PORT",
  "owner": "stove0-exiftool-observer"
}
```

</details>
