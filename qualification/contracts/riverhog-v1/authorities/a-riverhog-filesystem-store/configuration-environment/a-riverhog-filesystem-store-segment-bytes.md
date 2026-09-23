# A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-segment-bytes:ad17cfe52b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8af4063a2b"></a>

| Field | Value |
|---|---|
| <a id="s-780fea1d0c"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-81227f6e06"></a>`default_expressions` | `["''"]` |
| <a id="s-2760bd28e2"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES"` |
| <a id="s-4a2f875513"></a>`input_shape` | `"environment-string"` |
| <a id="s-9feb87a0c4"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES"` |
| <a id="s-095861fb48"></a>`owner` | `"a-riverhog-filesystem-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES"; consumers=["a-riverhog-filesystem-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES](#s-8af4063a2b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b8c6d9b404"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-595be84be6"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES](../../../evidence/sources/authorities.md#src-8068b649d4) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/108`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d634d9c9bdae5c0bf4e345edb9476d380beb5c2935b20381b1bbf3d3e6d5f0a8 -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_SEGMENT_BYTES",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
