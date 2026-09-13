# RIVERHOG_ARCHIVE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-stores:90252c9be9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c80b060880"></a>
| Field | Shape |
|---|---|
| <a id="s-fe8922d859"></a>`consumers` | ["riverhog-server"] |
| <a id="s-63325e3b83"></a>`default_expressions` | ["'archive'"] |
| <a id="s-8546fde067"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_STORES" |
| <a id="s-2ae46bdb0a"></a>`input_shape` | "environment-string" |
| <a id="s-e9729640af"></a>`name` | "RIVERHOG_ARCHIVE_STORES" |
| <a id="s-09d7007204"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-fc829ed686"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_STORES](../../../evidence/sources.md#src-b4408185b7) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `values.get('RIVERHOG_ARCHIVE_STORES', 'archive')` |

### Machine authority

- `/external_contract/configuration_environment/43`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7deb739b33be792436d183b57c32c48615a1b923426a1bc5a6a3bac827aaef8 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'archive'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_STORES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_STORES",
  "owner": "riverhog-server"
}
```
