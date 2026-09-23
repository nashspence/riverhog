# A_RIVERHOG_FILESYSTEM_STORE_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-host:53e5151f5e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9f9eafd5d5"></a>

| Field | Value |
|---|---|
| <a id="s-8248147251"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-be154e911d"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-8211605975"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_HOST"` |
| <a id="s-c550564cdf"></a>`input_shape` | `"environment-string"` |
| <a id="s-0e24792ee3"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_HOST"` |
| <a id="s-48214e7d5d"></a>`owner` | `"a-riverhog-filesystem-store"` |

## Governing policies

- <a id="pa-58af7fbbfa"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_HOST](../../../evidence/sources/authorities.md#src-2f52afaedc) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(f'{_PREFIX}HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/103`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b43b797e7417ad94abff2b4f98f104e533ea6965793a1e08bde298740b1b256 -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_HOST",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_HOST",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
