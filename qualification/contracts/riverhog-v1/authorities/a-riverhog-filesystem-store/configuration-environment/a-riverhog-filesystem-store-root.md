# A_RIVERHOG_FILESYSTEM_STORE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-root:cfaba395f7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b002419278"></a>

| Field | Value |
|---|---|
| <a id="s-e3ac27b7d7"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-a3b4239e3f"></a>`default_expressions` | `["''"]` |
| <a id="s-42e9fbb861"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_ROOT"` |
| <a id="s-dc370af86b"></a>`input_shape` | `"environment-string"` |
| <a id="s-058d8b8874"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_ROOT"` |
| <a id="s-46978b3ca6"></a>`owner` | `"a-riverhog-filesystem-store"` |

## Governing policies

- <a id="pa-32ab95b05e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_ROOT](../../../evidence/sources/authorities.md#src-090033eeb5) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_required](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/107`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e618274c14181da7c21aa388678df7967d03e3f2da813a12ef63c428a8743f0c -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_ROOT",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_ROOT",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
