# A_STOVE0_EXIFTOOL_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:a-stove0-exiftool-observer-token:8dd96b7482 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f3dc096fc3"></a>

| Field | Value |
|---|---|
| <a id="s-89ecd9cd3b"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-26ca0734d7"></a>`default_expressions` | `["unset"]` |
| <a id="s-201310b467"></a>`id` | `"a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_TOKEN"` |
| <a id="s-df24c034cb"></a>`input_shape` | `"environment-string"` |
| <a id="s-4baab6d3a2"></a>`name` | `"A_STOVE0_EXIFTOOL_OBSERVER_TOKEN"` |
| <a id="s-7580b0bc08"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-3d7f5ab40b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-exiftool-observer:A_STOVE0_EXIFTOOL_OBSERVER_TOKEN](../../../evidence/sources/authorities.md#src-a30c8b4892) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::\_secret](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py); [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::main](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.getenv('A_STOVE0_EXIFTOOL_OBSERVER_TOKEN')` |
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.environ.pop('A_STOVE0_EXIFTOOL_OBSERVER_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/123`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0776cbd6529a612a6c8f162df3cee5a5ff325e0f0125d7723a4d3e9c6159424 -->

```json
{
  "consumers": [
    "a-stove0-exiftool-observer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_TOKEN",
  "input_shape": "environment-string",
  "name": "A_STOVE0_EXIFTOOL_OBSERVER_TOKEN",
  "owner": "a-stove0-exiftool-observer"
}
```

</details>
