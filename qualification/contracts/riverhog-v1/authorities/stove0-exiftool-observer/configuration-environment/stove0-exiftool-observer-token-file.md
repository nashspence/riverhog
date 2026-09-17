# STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-token-file:c7fd0d90a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3f89961eb2"></a>

| Field | Value |
|---|---|
| <a id="s-67e906ba4c"></a>`consumers` | `["stove0-exiftool-observer"]` |
| <a id="s-bff9a69f3a"></a>`default_expressions` | `["unset"]` |
| <a id="s-96bdabf9ee"></a>`id` | `"stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE"` |
| <a id="s-6d52655fc2"></a>`input_shape` | `"environment-string"` |
| <a id="s-a96edc8bcf"></a>`name` | `"STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE"` |
| <a id="s-749493ebc6"></a>`owner` | `"stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-792d186ed1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE](../../../evidence/sources.md#src-7e8e1ce139) — [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py::\_secret](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py) | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/154`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 038b8faf0e77ed4a2fe42d76e09d408bb2d645a5c067c1b0cf8eeaa9c1c33a22 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE",
  "owner": "stove0-exiftool-observer"
}
```

</details>
