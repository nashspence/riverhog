# STOVE0_OPUS_TARGET_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-token-file:15b1193eaa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6dde132904"></a>

| Field | Value |
|---|---|
| <a id="s-f8e4d1c1e8"></a>`consumers` | `["stove0-opus-target"]` |
| <a id="s-9fcf455451"></a>`default_expressions` | `["unset"]` |
| <a id="s-cd775f77c0"></a>`id` | `"stove0-opus-target:environment:STOVE0_OPUS_TARGET_TOKEN_FILE"` |
| <a id="s-57e15b45fe"></a>`input_shape` | `"environment-string"` |
| <a id="s-bb8c4184e6"></a>`name` | `"STOVE0_OPUS_TARGET_TOKEN_FILE"` |
| <a id="s-6616e13dd1"></a>`owner` | `"stove0-opus-target"` |

## Governing policies

- <a id="pa-eef39f6783"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_TOKEN_FILE](../../../evidence/sources.md#src-0d6bd47b40) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv(f'{prefix}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/197`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8eaf71ccb500f6986dcb0f5ff646f3ed7beffe6fe02bdc5f7f8c105e71f192a -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_TOKEN_FILE",
  "owner": "stove0-opus-target"
}
```

</details>
