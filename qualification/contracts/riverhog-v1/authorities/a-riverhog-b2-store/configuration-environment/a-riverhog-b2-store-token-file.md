# A_RIVERHOG_B2_STORE_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-token-file:c46efe4d45 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d15fb4b928"></a>

| Field | Value |
|---|---|
| <a id="s-98b9d4f5c3"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-a6c332bf33"></a>`default_expressions` | `["unset"]` |
| <a id="s-7c79245e59"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_TOKEN_FILE"` |
| <a id="s-32b8dc3225"></a>`input_shape` | `"environment-string"` |
| <a id="s-a54012d8c0"></a>`name` | `"A_RIVERHOG_B2_STORE_TOKEN_FILE"` |
| <a id="s-fe903fc9c1"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-58c1423f2d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_TOKEN_FILE](../../../evidence/sources/authorities.md#src-bff1f37265) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/95`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ecfe521cd4c403bdcda4a24b1e2501bede879f533bdd11802165103c428153e -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_TOKEN_FILE",
  "owner": "a-riverhog-b2-store"
}
```

</details>
