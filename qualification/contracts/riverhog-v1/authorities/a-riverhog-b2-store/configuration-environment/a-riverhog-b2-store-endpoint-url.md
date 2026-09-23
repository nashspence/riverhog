# A_RIVERHOG_B2_STORE_ENDPOINT_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-endpoint-url:1ade1eaecb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-24fc48da99"></a>

| Field | Value |
|---|---|
| <a id="s-fa86d7a910"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-1b714bd214"></a>`default_expressions` | `["''"]` |
| <a id="s-0452f51736"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ENDPOINT_URL"` |
| <a id="s-cb1d246623"></a>`input_shape` | `"environment-string"` |
| <a id="s-071aa15ddd"></a>`name` | `"A_RIVERHOG_B2_STORE_ENDPOINT_URL"` |
| <a id="s-40f3826282"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-76e9a42aef"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_ENDPOINT_URL](../../../evidence/sources/authorities.md#src-344c447470) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_required](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/80`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25f44b129ae6a7a153d4001af843b54db78b95c8dfe12e30ae718b5d33feebc5 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ENDPOINT_URL",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_ENDPOINT_URL",
  "owner": "a-riverhog-b2-store"
}
```

</details>
