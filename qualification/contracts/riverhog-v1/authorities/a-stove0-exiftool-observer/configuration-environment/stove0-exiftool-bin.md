# STOVE0_EXIFTOOL_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:stove0-exiftool-bin:11f085ba90 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-91af6fdd5c"></a>

| Field | Value |
|---|---|
| <a id="s-b3f49ca63f"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-915d92d4ad"></a>`default_expressions` | `["'exiftool'"]` |
| <a id="s-202000a564"></a>`id` | `"a-stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_BIN"` |
| <a id="s-7d40f9b692"></a>`input_shape` | `"environment-string"` |
| <a id="s-814b85f667"></a>`name` | `"STOVE0_EXIFTOOL_BIN"` |
| <a id="s-4cd44f5a55"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-d8d7697438"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-exiftool-observer:STOVE0_EXIFTOOL_BIN](../../../evidence/sources/authorities.md#src-343f41d30f) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::main](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.getenv('STOVE0_EXIFTOOL_BIN', 'exiftool')` |

### Machine authority

- `/external_contract/configuration_environment/70`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81a564e4a5b6fa7a485a99c064a95f625e029ff12963428f00eb971680203bca -->

```json
{
  "consumers": [
    "a-stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'exiftool'"
  ],
  "id": "a-stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_BIN",
  "owner": "a-stove0-exiftool-observer"
}
```

</details>
