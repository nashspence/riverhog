# A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-connect-timeout-seconds:5a66486102 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d42d9004a6"></a>

| Field | Value |
|---|---|
| <a id="s-ef9d7442ff"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-aecbad9c35"></a>`default_expressions` | `["''"]` |
| <a id="s-7b216e5267"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS"` |
| <a id="s-ce46db95b3"></a>`input_shape` | `"environment-string"` |
| <a id="s-c980de4ee4"></a>`name` | `"A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS"` |
| <a id="s-a9e838bc8b"></a>`owner` | `"a-riverhog-b2-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS"; consumers=["a-riverhog-b2-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS](#s-d42d9004a6) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3127a40fc6"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-30924fd4ca"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-5bea0d8926) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/79`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07b56c3a7a5f7b7c3c592f54b299e3422c329504f2edfbe20b84582f3f94cf0d -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_CONNECT_TIMEOUT_SECONDS",
  "owner": "a-riverhog-b2-store"
}
```

</details>
