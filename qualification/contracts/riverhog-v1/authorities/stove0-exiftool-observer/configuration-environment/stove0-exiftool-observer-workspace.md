# STOVE0_EXIFTOOL_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-workspace:d1bcac2d73 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1fc58a9ae0"></a>

| Field | Value |
|---|---|
| <a id="s-8016cb8611"></a>`consumers` | `["stove0-exiftool-observer"]` |
| <a id="s-900997bf8a"></a>`default_expressions` | `["'/run/stove0-exiftool-observer'"]` |
| <a id="s-e0bcee7d24"></a>`id` | `"stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE"` |
| <a id="s-c890ffe49d"></a>`input_shape` | `"environment-string"` |
| <a id="s-9b87afeb24"></a>`name` | `"STOVE0_EXIFTOOL_OBSERVER_WORKSPACE"` |
| <a id="s-76e9c7ea67"></a>`owner` | `"stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-8e28bcb1a9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE](../../../evidence/sources/authorities.md#src-98428ceea4) — [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py::main](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py) | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_WORKSPACE', '/run/stove0-exiftool-observer')` |

### Machine authority

- `/external_contract/configuration_environment/155`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9485ce5a6bd2a1d3771d86b2ed3cb512e9c64b383129b6cf1b8e376630b88f19 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'/run/stove0-exiftool-observer'"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_OBSERVER_WORKSPACE",
  "owner": "stove0-exiftool-observer"
}
```

</details>
