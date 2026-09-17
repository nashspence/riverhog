# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-new-archive-enabled:da863532a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-63134bf139"></a>

| Field | Value |
|---|---|
| <a id="s-7a2481d8f8"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-47a10e73f7"></a>`default_expressions` | `["'true'"]` |
| <a id="s-d1248cce4c"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED"` |
| <a id="s-970820b4ef"></a>`input_shape` | `"environment-string"` |
| <a id="s-822be7b829"></a>`name` | `"RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED"` |
| <a id="s-3c8df38f70"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-99b2bea9bc"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED](../../../evidence/sources/authorities.md#src-257c42be6d) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED', 'true')` |

### Machine authority

- `/external_contract/configuration_environment/69`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 632b4d881f4674537247f9536c41318090fe97a89973fe8b8c1715aa2ebc3c59 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'true'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED",
  "owner": "riverhog-server"
}
```

</details>
