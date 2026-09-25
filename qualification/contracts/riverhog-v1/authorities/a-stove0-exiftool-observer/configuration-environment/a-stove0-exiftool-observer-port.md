# A_STOVE0_EXIFTOOL_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:a-stove0-exiftool-observer-port:215c0d882e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5c633c982d"></a>

| Field | Value |
|---|---|
| <a id="s-093b7de81f"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-cb2ce64f5f"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-e6605379f6"></a>`id` | `"a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_PORT"` |
| <a id="s-1160de2df9"></a>`input_shape` | `"environment-string"` |
| <a id="s-340ef8e53f"></a>`name` | `"A_STOVE0_EXIFTOOL_OBSERVER_PORT"` |
| <a id="s-51770eadf0"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-aa0267c1a5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-exiftool-observer:A_STOVE0_EXIFTOOL_OBSERVER_PORT](../../../evidence/sources/authorities.md#src-058770785d) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::\_parser](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.getenv('A_STOVE0_EXIFTOOL_OBSERVER_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/65`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f850f4f035e450606489556a6e04d703b1f2f661f410a8daf265351cced410be -->

```json
{
  "consumers": [
    "a-stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_PORT",
  "input_shape": "environment-string",
  "name": "A_STOVE0_EXIFTOOL_OBSERVER_PORT",
  "owner": "a-stove0-exiftool-observer"
}
```

</details>
