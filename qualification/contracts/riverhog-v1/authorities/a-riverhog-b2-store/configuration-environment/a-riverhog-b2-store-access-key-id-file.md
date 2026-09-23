# A_RIVERHOG_B2_STORE_ACCESS_KEY_ID_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-access-key-id-file:f0837e01a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c741de7c16"></a>

| Field | Value |
|---|---|
| <a id="s-3954c61aa8"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-56d7952e75"></a>`default_expressions` | `["unset"]` |
| <a id="s-e3fea57c53"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ACCESS_KEY_ID_FILE"` |
| <a id="s-fc18258489"></a>`input_shape` | `"environment-string"` |
| <a id="s-ee62ea9044"></a>`name` | `"A_RIVERHOG_B2_STORE_ACCESS_KEY_ID_FILE"` |
| <a id="s-175cffa6ac"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-4dc3083f5d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_ACCESS_KEY_ID_FILE](../../../evidence/sources/authorities.md#src-7d4314e795) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/77`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b3d043401dd7e1b8196337f98422873a6d5193f0a8cac80780a8c5d5cf2f7df -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ACCESS_KEY_ID_FILE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_ACCESS_KEY_ID_FILE",
  "owner": "a-riverhog-b2-store"
}
```

</details>
