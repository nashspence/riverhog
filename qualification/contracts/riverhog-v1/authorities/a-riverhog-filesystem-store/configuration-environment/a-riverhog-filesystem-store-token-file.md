# A_RIVERHOG_FILESYSTEM_STORE_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-token-file:234fb93ccf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6cd1741870"></a>

| Field | Value |
|---|---|
| <a id="s-f89c1c3048"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-4209677621"></a>`default_expressions` | `["unset"]` |
| <a id="s-ee9ed3533d"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_TOKEN_FILE"` |
| <a id="s-42c7b7c437"></a>`input_shape` | `"environment-string"` |
| <a id="s-7027ddbe71"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_TOKEN_FILE"` |
| <a id="s-5b4c4db475"></a>`owner` | `"a-riverhog-filesystem-store"` |

## Governing policies

- <a id="pa-be03593a7f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_TOKEN_FILE](../../../evidence/sources/authorities.md#src-ec21ba3b7a) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/110`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 745ce3e8073a8fc2eee4a6f49b57ee2a711ceb32104892e7bdcd0508a28c4d5b -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_TOKEN_FILE",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
