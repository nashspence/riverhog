# A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-read-timeout-seconds:10939a6707 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-41e02220a1"></a>

| Field | Value |
|---|---|
| <a id="s-84903db1b5"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-305c1a2132"></a>`default_expressions` | `["''"]` |
| <a id="s-db7c90b69a"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS"` |
| <a id="s-4c733e3e28"></a>`input_shape` | `"environment-string"` |
| <a id="s-f3010b390a"></a>`name` | `"A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS"` |
| <a id="s-8d1cbb3d58"></a>`owner` | `"a-riverhog-b2-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS"; consumers=["a-riverhog-b2-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS](#s-41e02220a1) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c1ea26c06b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-d3e2ea9afd"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-79203c16a5) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/87`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c37f3a5a0737951b7a779373b5234ad464d51e4ceef08d4b01635b6c549f21e -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_READ_TIMEOUT_SECONDS",
  "owner": "a-riverhog-b2-store"
}
```

</details>
