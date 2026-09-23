# A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-max-pool-connections:4315bcf11d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-47fb606ae5"></a>

| Field | Value |
|---|---|
| <a id="s-c114306576"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-f48a7336e8"></a>`default_expressions` | `["''"]` |
| <a id="s-8f3b8fe1e3"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS"` |
| <a id="s-180063e963"></a>`input_shape` | `"environment-string"` |
| <a id="s-e3159c3eeb"></a>`name` | `"A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS"` |
| <a id="s-065409c482"></a>`owner` | `"a-riverhog-b2-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS"; consumers=["a-riverhog-b2-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS](#s-47fb606ae5) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-46a850e069"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-add0a37204"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS](../../../evidence/sources/authorities.md#src-ef7aa29cb1) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/84`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 084386d306a0f55fd3fda89dbe701fc510300e708f2f669af00e3f84396b4316 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_MAX_POOL_CONNECTIONS",
  "owner": "a-riverhog-b2-store"
}
```

</details>
