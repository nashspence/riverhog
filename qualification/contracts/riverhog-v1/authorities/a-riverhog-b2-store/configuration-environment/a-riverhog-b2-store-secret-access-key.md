# A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-secret-access-key:57e93630f7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-74f1ce0aa1"></a>

| Field | Value |
|---|---|
| <a id="s-14c637bf7d"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-438a310e71"></a>`default_expressions` | `["unset"]` |
| <a id="s-5670701bbc"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY"` |
| <a id="s-5218f9ba8a"></a>`input_shape` | `"environment-string"` |
| <a id="s-9911339263"></a>`name` | `"A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY"` |
| <a id="s-a4a31c47b8"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-65e546a9f7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY](../../../evidence/sources/authorities.md#src-45dcde2670) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/91`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b7dfc9c1e85a2cc362cf452eacdd31073f45d1d4156efec549feab92419f183 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY",
  "owner": "a-riverhog-b2-store"
}
```

</details>
