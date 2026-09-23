# A_RIVERHOG_B2_STORE_RETRY_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-retry-mode:df414512bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3a00485a0d"></a>

| Field | Value |
|---|---|
| <a id="s-f4162f50d7"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-64cdad0f75"></a>`default_expressions` | `["''"]` |
| <a id="s-c084c603a4"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_RETRY_MODE"` |
| <a id="s-198c63854c"></a>`input_shape` | `"environment-string"` |
| <a id="s-811edc1b84"></a>`name` | `"A_RIVERHOG_B2_STORE_RETRY_MODE"` |
| <a id="s-a77e047c01"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-fb12b98794"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_RETRY_MODE](../../../evidence/sources/authorities.md#src-efdac7ea5b) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/89`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09113c2a27276bd50ae1118be2fdebc0f90f138394a32d726f7db7b8ca5c02b7 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_RETRY_MODE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_RETRY_MODE",
  "owner": "a-riverhog-b2-store"
}
```

</details>
