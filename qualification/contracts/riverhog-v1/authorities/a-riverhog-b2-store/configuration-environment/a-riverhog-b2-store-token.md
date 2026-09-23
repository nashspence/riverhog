# A_RIVERHOG_B2_STORE_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-token:f51c46a3f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b5b56da79c"></a>

| Field | Value |
|---|---|
| <a id="s-c4c426b9e4"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-3371f237e2"></a>`default_expressions` | `["unset"]` |
| <a id="s-f366076fc7"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_TOKEN"` |
| <a id="s-8ecc30e0a1"></a>`input_shape` | `"environment-string"` |
| <a id="s-953e2f4a91"></a>`name` | `"A_RIVERHOG_B2_STORE_TOKEN"` |
| <a id="s-ba60d64f0b"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-44107b9c26"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_TOKEN](../../../evidence/sources/authorities.md#src-859399a5ca) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/94`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2269908c41923913b23f785a8e0d6331472b6c813f7070d3e58e961f1f26b303 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_TOKEN",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_TOKEN",
  "owner": "a-riverhog-b2-store"
}
```

</details>
