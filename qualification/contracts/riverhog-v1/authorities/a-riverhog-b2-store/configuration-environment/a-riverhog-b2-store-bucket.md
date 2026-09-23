# A_RIVERHOG_B2_STORE_BUCKET

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-bucket:57b1704ba9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3efbdd0d50"></a>

| Field | Value |
|---|---|
| <a id="s-e5a8456801"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-7044fe0ae8"></a>`default_expressions` | `["''"]` |
| <a id="s-c4e21ab4c8"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_BUCKET"` |
| <a id="s-ee31f3135d"></a>`input_shape` | `"environment-string"` |
| <a id="s-8bf9d9fca2"></a>`name` | `"A_RIVERHOG_B2_STORE_BUCKET"` |
| <a id="s-33c76d2bc9"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-17c6e02375"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_BUCKET](../../../evidence/sources/authorities.md#src-483ff8cc29) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_required](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/78`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5c4515d10a5da2906fd16deee394f26e6d4fc3c2ccbb16e6ef41a3813979c87 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_BUCKET",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_BUCKET",
  "owner": "a-riverhog-b2-store"
}
```

</details>
