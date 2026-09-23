# A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-secret-access-key-file:46d38e3213 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-651129acd1"></a>

| Field | Value |
|---|---|
| <a id="s-0a8dd8ad5b"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-506bc75a04"></a>`default_expressions` | `["unset"]` |
| <a id="s-448ec91883"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY_FILE"` |
| <a id="s-49c9216ceb"></a>`input_shape` | `"environment-string"` |
| <a id="s-b54b400817"></a>`name` | `"A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY_FILE"` |
| <a id="s-8ded7f4c58"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-d23417e6e0"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY_FILE](../../../evidence/sources/authorities.md#src-41b69928e4) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/92`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2cc13420cd5494dd618e694ea43e97d192b28ca8b964ea3beb0964cdbda8432 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY_FILE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY_FILE",
  "owner": "a-riverhog-b2-store"
}
```

</details>
