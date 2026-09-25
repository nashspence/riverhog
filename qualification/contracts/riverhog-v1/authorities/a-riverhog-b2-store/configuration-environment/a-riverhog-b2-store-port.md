# A_RIVERHOG_B2_STORE_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-port:89c135542d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b200cddc47"></a>

| Field | Value |
|---|---|
| <a id="s-a64ea888cd"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-45ea5bbfc8"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-114bfa5509"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_PORT"` |
| <a id="s-e3a22daa43"></a>`input_shape` | `"environment-string"` |
| <a id="s-5b43242159"></a>`name` | `"A_RIVERHOG_B2_STORE_PORT"` |
| <a id="s-3435db812d"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-9f62f668fc"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_PORT](../../../evidence/sources/authorities.md#src-ce9319f327) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/41`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75fb0b45faf8bb9c2eb414d14ffc8eaf2c90b7df443007ccc906741af4f3a6b4 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_PORT",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_PORT",
  "owner": "a-riverhog-b2-store"
}
```

</details>
