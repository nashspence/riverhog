# A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-read-chunk-bytes:a66347c8ee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-62bc3f7cc9"></a>

| Field | Value |
|---|---|
| <a id="s-119f21dafe"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-3c87eac384"></a>`default_expressions` | `["''"]` |
| <a id="s-01069a2d81"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES"` |
| <a id="s-a900a6f3a0"></a>`input_shape` | `"environment-string"` |
| <a id="s-02fadb9af0"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES"` |
| <a id="s-6ab5fa12ac"></a>`owner` | `"a-riverhog-filesystem-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES"; consumers=["a-riverhog-filesystem-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES](#s-62bc3f7cc9) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0078e77c0a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-e7d220fd1b"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES](../../../evidence/sources/authorities.md#src-cb6705aa16) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/106`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 659e0ddf969bfd6cd2b7f346d7309a9aaabf423305f2e0f69935dc44f9d2b770 -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_READ_CHUNK_BYTES",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
