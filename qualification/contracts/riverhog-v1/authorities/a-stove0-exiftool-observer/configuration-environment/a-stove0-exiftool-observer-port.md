# A_STOVE0_EXIFTOOL_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:a-stove0-exiftool-observer-port:4d863fa014 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7109d2d380"></a>

| Field | Value |
|---|---|
| <a id="s-cb034d2b66"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-139d07d0a8"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-96289d1b72"></a>`id` | `"a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_PORT"` |
| <a id="s-9c6d8c3217"></a>`input_shape` | `"environment-string"` |
| <a id="s-29050475a4"></a>`name` | `"A_STOVE0_EXIFTOOL_OBSERVER_PORT"` |
| <a id="s-26a08e21d8"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-1a98604239"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/121`

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
