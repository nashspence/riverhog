# A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-read-chunk-bytes:506a304be8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-284bbb4914"></a>

| Field | Value |
|---|---|
| <a id="s-c9aa0936f8"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-bb482cec50"></a>`default_expressions` | `["''"]` |
| <a id="s-f91da4b941"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES"` |
| <a id="s-f863ec0947"></a>`input_shape` | `"environment-string"` |
| <a id="s-9f2399bb48"></a>`name` | `"A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES"` |
| <a id="s-1932bf0a08"></a>`owner` | `"a-riverhog-b2-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES"; consumers=["a-riverhog-b2-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES](#s-284bbb4914) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f93b815380"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-ba6cea8c04"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES](../../../evidence/sources/authorities.md#src-25185bfa05) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/86`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee167000d5e4a5b780ab22494b89e9b2109f9990239b14ab1b0a814ed93135e8 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_READ_CHUNK_BYTES",
  "owner": "a-riverhog-b2-store"
}
```

</details>
