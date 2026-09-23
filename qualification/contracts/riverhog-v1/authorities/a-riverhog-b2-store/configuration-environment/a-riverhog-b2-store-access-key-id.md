# A_RIVERHOG_B2_STORE_ACCESS_KEY_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-access-key-id:c82dfbe213 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0e8ce1fd7c"></a>

| Field | Value |
|---|---|
| <a id="s-8e543c475e"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-cfec5ebc31"></a>`default_expressions` | `["unset"]` |
| <a id="s-c7d082cc1f"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ACCESS_KEY_ID"` |
| <a id="s-8dde74de5f"></a>`input_shape` | `"environment-string"` |
| <a id="s-38ce56b288"></a>`name` | `"A_RIVERHOG_B2_STORE_ACCESS_KEY_ID"` |
| <a id="s-efc59f7a1f"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-e37fd84d0b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_ACCESS_KEY_ID](../../../evidence/sources/authorities.md#src-01a69b2499) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/76`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f3f2bec80e37c457f8692380d9b7319d3025fac967a7c79711bdff2a4b9a77a -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ACCESS_KEY_ID",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_ACCESS_KEY_ID",
  "owner": "a-riverhog-b2-store"
}
```

</details>
