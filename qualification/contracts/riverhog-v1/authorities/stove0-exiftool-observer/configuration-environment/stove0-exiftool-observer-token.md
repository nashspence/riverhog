# STOVE0_EXIFTOOL_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-token:6f3a315a7a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e5598ea0d6"></a>

| Field | Value |
|---|---|
| <a id="s-4b0a5cb145"></a>`consumers` | `["stove0-exiftool-observer"]` |
| <a id="s-bb69afeff8"></a>`default_expressions` | `["unset"]` |
| <a id="s-6097d28502"></a>`id` | `"stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN"` |
| <a id="s-a1ce093d0a"></a>`input_shape` | `"environment-string"` |
| <a id="s-22c3b4e1d8"></a>`name` | `"STOVE0_EXIFTOOL_OBSERVER_TOKEN"` |
| <a id="s-a4aef99b1b"></a>`owner` | `"stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-3ce0671aba"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN](../../../evidence/sources.md#src-5cf4e00bd2) — [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py::\_secret](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py); [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py::main](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py) | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_TOKEN')` |
| parser | `stove0-exiftool-observer` | [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py) | `os.environ.pop('STOVE0_EXIFTOOL_OBSERVER_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/153`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd68c3c4681d2a2554c251323c1c563077a0d86e3fed50c5060d5b1a2960d1a1 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN",
  "owner": "stove0-exiftool-observer"
}
```

</details>
