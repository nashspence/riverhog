# A_RIVERHOG_B2_STORE_REGION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-region:548ed4b036 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5fe6983476"></a>

| Field | Value |
|---|---|
| <a id="s-939e46bead"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-4e50679c67"></a>`default_expressions` | `["''"]` |
| <a id="s-46a05f24bc"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_REGION"` |
| <a id="s-0ce1d91389"></a>`input_shape` | `"environment-string"` |
| <a id="s-f7d158a894"></a>`name` | `"A_RIVERHOG_B2_STORE_REGION"` |
| <a id="s-0050bf8e14"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-a9cae2ed37"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_REGION](../../../evidence/sources/authorities.md#src-4d26ea602e) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_required](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/88`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47c6f94ba9ef0eff5d120746646c6cf725e1c488c308ba5e0ce7fe3c82d41de9 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_REGION",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_REGION",
  "owner": "a-riverhog-b2-store"
}
```

</details>
