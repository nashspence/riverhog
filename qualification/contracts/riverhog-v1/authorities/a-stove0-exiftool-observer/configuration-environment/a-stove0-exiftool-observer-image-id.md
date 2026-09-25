# A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:a-stove0-exiftool-observer-image-id:24c5bd6c7c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1c368e3bbb"></a>

| Field | Value |
|---|---|
| <a id="s-0e717d7f5c"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-ed83d1071c"></a>`default_expressions` | `["''"]` |
| <a id="s-8acc408344"></a>`id` | `"a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID"` |
| <a id="s-18b068f2a0"></a>`input_shape` | `"environment-string"` |
| <a id="s-561f90128c"></a>`name` | `"A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID"` |
| <a id="s-2750bfabee"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-0a83c6a858"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-exiftool-observer:A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID](../../../evidence/sources/authorities.md#src-c1511e1fac) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::\_image\_id](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.getenv('A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID', '')` |

### Machine authority

- `/external_contract/configuration_environment/64`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83e9139b6682731c9cf99c47021d1e097677b05568295f7c7b392751f9fb1803 -->

```json
{
  "consumers": [
    "a-stove0-exiftool-observer"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID",
  "input_shape": "environment-string",
  "name": "A_STOVE0_EXIFTOOL_OBSERVER_IMAGE_ID",
  "owner": "a-stove0-exiftool-observer"
}
```

</details>
