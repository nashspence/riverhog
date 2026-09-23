# A_STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:a-stove0-exiftool-observer-source-revision:77b3a0fe8b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1c81252996"></a>

| Field | Value |
|---|---|
| <a id="s-841c1e0c0e"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-b96c482685"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-ae1c3e3714"></a>`id` | `"a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION"` |
| <a id="s-74bf540ff3"></a>`input_shape` | `"environment-string"` |
| <a id="s-cfc1e16201"></a>`name` | `"A_STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION"` |
| <a id="s-f76569877b"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-64ce926e7b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-exiftool-observer:A_STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION](../../../evidence/sources/authorities.md#src-c19ae4121d) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::main](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.getenv('A_STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/122`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84c84e84d0fd51c445cc61d824e1dc82ede713c8ae1153c385776c3c730361e1 -->

```json
{
  "consumers": [
    "a-stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "A_STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION",
  "owner": "a-stove0-exiftool-observer"
}
```

</details>
