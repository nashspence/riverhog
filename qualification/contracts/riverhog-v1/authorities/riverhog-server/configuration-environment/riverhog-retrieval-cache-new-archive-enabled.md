# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-new-archive-enabled:da863532a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-63134bf139"></a>
| Field | Shape |
|---|---|
| <a id="s-7a2481d8f8"></a>`consumers` | ["riverhog-server"] |
| <a id="s-47a10e73f7"></a>`default_expressions` | ["'true'"] |
| <a id="s-d1248cce4c"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED" |
| <a id="s-970820b4ef"></a>`input_shape` | "environment-string" |
| <a id="s-822be7b829"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED" |
| <a id="s-3c8df38f70"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-99b2bea9bc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED](../../../evidence/sources.md#src-257c42be6d) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED', 'true')` |

### Machine authority

- `/external_contract/configuration_environment/69`

### Exact owned JSON

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
