# STOVE0_EXIFTOOL_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-bin:e05690ac45 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d6e9a2d362"></a>

| Field | Value |
|---|---|
| <a id="s-3f756a949d"></a>`consumers` | `["stove0-exiftool-observer"]` |
| <a id="s-519e87b83b"></a>`default_expressions` | `["'exiftool'"]` |
| <a id="s-28d94b8700"></a>`id` | `"stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_BIN"` |
| <a id="s-4777e8907c"></a>`input_shape` | `"environment-string"` |
| <a id="s-cbc0ee4f3e"></a>`name` | `"STOVE0_EXIFTOOL_BIN"` |
| <a id="s-70282cfd79"></a>`owner` | `"stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-9647efa995"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_BIN](../../../evidence/sources/authorities.md#src-db3b5f3ab1) — [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py::main](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py) | `os.getenv('STOVE0_EXIFTOOL_BIN', 'exiftool')` |

### Machine authority

- `/external_contract/configuration_environment/148`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56fad947b9d01eeea2c5a356fd29fa0d921a33ad2a76b10aadd063bca07ee339 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'exiftool'"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_BIN",
  "owner": "stove0-exiftool-observer"
}
```

</details>
