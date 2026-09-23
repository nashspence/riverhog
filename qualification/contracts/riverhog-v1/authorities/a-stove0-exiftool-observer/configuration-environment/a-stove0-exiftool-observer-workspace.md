# A_STOVE0_EXIFTOOL_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:a-stove0-exiftool-observer-workspace:e39a0232b7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-25dea1f42a"></a>

| Field | Value |
|---|---|
| <a id="s-648007d91b"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-96a6dd7dd7"></a>`default_expressions` | `["'/run/a-stove0-exiftool-observer'"]` |
| <a id="s-8ff65ad249"></a>`id` | `"a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_WORKSPACE"` |
| <a id="s-61c4702084"></a>`input_shape` | `"environment-string"` |
| <a id="s-79164d1f48"></a>`name` | `"A_STOVE0_EXIFTOOL_OBSERVER_WORKSPACE"` |
| <a id="s-5908522b13"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-5f176f05f0"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-exiftool-observer:A_STOVE0_EXIFTOOL_OBSERVER_WORKSPACE](../../../evidence/sources/authorities.md#src-1b67d130ad) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::main](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.getenv('A_STOVE0_EXIFTOOL_OBSERVER_WORKSPACE', '/run/a-stove0-exiftool-observer')` |

### Machine authority

- `/external_contract/configuration_environment/125`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0cea4d32d96597d7b8646f278ec184bafd663ed2f97f6124dd61b8414322b7e4 -->

```json
{
  "consumers": [
    "a-stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'/run/a-stove0-exiftool-observer'"
  ],
  "id": "a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_WORKSPACE",
  "input_shape": "environment-string",
  "name": "A_STOVE0_EXIFTOOL_OBSERVER_WORKSPACE",
  "owner": "a-stove0-exiftool-observer"
}
```

</details>
