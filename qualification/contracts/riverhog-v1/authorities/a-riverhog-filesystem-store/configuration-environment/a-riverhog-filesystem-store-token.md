# A_RIVERHOG_FILESYSTEM_STORE_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-token:6b19cb30f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-63f2a8f8ec"></a>

| Field | Value |
|---|---|
| <a id="s-7635dc7bce"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-9adaac50e0"></a>`default_expressions` | `["unset"]` |
| <a id="s-5ba55aed0e"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_TOKEN"` |
| <a id="s-74c255c80c"></a>`input_shape` | `"environment-string"` |
| <a id="s-0b8c0a3aae"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_TOKEN"` |
| <a id="s-26fd49b762"></a>`owner` | `"a-riverhog-filesystem-store"` |

## Governing policies

- <a id="pa-d98d22b071"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_TOKEN](../../../evidence/sources/authorities.md#src-bd3131462e) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/109`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95f5b2b6da91c800c11f4a260e2b77591178eb57f66a042e14889b0015005eec -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_TOKEN",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_TOKEN",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
