# A_RIVERHOG_B2_STORE_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-host:2cf11bbb5e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c26bd33aa8"></a>

| Field | Value |
|---|---|
| <a id="s-cbfe80a062"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-8d23212c1a"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-a3f3fe3044"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_HOST"` |
| <a id="s-7d81361521"></a>`input_shape` | `"environment-string"` |
| <a id="s-501ec97f37"></a>`name` | `"A_RIVERHOG_B2_STORE_HOST"` |
| <a id="s-06c79d612b"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-2173006a61"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_HOST](../../../evidence/sources/authorities.md#src-b1a113a778) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/40`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a5e5a4499840585e1632bba15371b2244444eab7132584786f42f6a6097bb9f -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_HOST",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_HOST",
  "owner": "a-riverhog-b2-store"
}
```

</details>
