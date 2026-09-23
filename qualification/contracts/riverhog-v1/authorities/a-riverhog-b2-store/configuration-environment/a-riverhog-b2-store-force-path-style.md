# A_RIVERHOG_B2_STORE_FORCE_PATH_STYLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-force-path-style:bd7fd2c34c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-205dff964b"></a>

| Field | Value |
|---|---|
| <a id="s-668719968c"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-c78f34962b"></a>`default_expressions` | `["''"]` |
| <a id="s-8a0133ded5"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_FORCE_PATH_STYLE"` |
| <a id="s-aa83bf285f"></a>`input_shape` | `"environment-string"` |
| <a id="s-663968b1c7"></a>`name` | `"A_RIVERHOG_B2_STORE_FORCE_PATH_STYLE"` |
| <a id="s-d6a05173f7"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-7f65c6bc60"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_FORCE_PATH_STYLE](../../../evidence/sources/authorities.md#src-645fdbcf90) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/81`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74d2373e3a7d7589575d3e893b6e7499d0d4e08d598c822cbf7cdf815d67172d -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_FORCE_PATH_STYLE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_FORCE_PATH_STYLE",
  "owner": "a-riverhog-b2-store"
}
```

</details>
