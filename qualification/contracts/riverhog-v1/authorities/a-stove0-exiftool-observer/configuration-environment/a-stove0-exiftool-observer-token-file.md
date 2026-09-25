# A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-exiftool-observer:a-stove0-exiftool-observer-token-file:bc55f3ad9d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3bf78de3fa"></a>

| Field | Value |
|---|---|
| <a id="s-9364d1a9f1"></a>`consumers` | `["a-stove0-exiftool-observer"]` |
| <a id="s-92b6a91362"></a>`default_expressions` | `["unset"]` |
| <a id="s-6a8524b81a"></a>`id` | `"a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE"` |
| <a id="s-594729b07d"></a>`input_shape` | `"environment-string"` |
| <a id="s-990d2b0cff"></a>`name` | `"A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE"` |
| <a id="s-5b3245d215"></a>`owner` | `"a-stove0-exiftool-observer"` |

## Governing policies

- <a id="pa-5872cb2298"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-exiftool-observer:A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE](../../../evidence/sources/authorities.md#src-5e11ab4d5a) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::\_secret](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-exiftool-observer` | [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py) | `os.getenv('A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/68`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50cc1f89014bfbeaed74a736c6e3ecf1f409de34c7554bb821bf82a7e7beb24a -->

```json
{
  "consumers": [
    "a-stove0-exiftool-observer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-stove0-exiftool-observer:environment:A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE",
  "owner": "a-stove0-exiftool-observer"
}
```

</details>
