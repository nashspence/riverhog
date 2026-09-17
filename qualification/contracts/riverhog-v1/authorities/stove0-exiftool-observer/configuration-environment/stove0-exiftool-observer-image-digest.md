# STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-image-digest:ff44fd4882 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-08f1419e44"></a>

| Field | Value |
|---|---|
| <a id="s-31ef2dba16"></a>`consumers` | `["stove0-exiftool-observer"]` |
| <a id="s-d3fe509e33"></a>`default_expressions` | `["''"]` |
| <a id="s-2e729be853"></a>`id` | `"stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST"` |
| <a id="s-f6e2dd3a40"></a>`input_shape` | `"environment-string"` |
| <a id="s-2cd6cd9947"></a>`name` | `"STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST"` |
| <a id="s-3602620722"></a>`owner` | `"stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-a44ab9ff7c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-e2f881a54b) — [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py::\_image\_digest](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/app.py](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py) | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/150`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5f7854415e9986fe7b7d765e956968b143ea471e5bf4d422a3eabb592e54003 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST",
  "owner": "stove0-exiftool-observer"
}
```

</details>
