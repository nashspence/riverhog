# RIVERHOG_LOG_LEVEL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-log-level:befef3cc2f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-52addf4ec4"></a>
| Field | Shape |
|---|---|
| <a id="s-d18a045679"></a>`consumers` | ["riverhog-server"] |
| <a id="s-ae4e26457f"></a>`default_expressions` | ["DEFAULT_LOG_LEVEL"] |
| <a id="s-769fe4c6ca"></a>`id` | "riverhog-server:environment:RIVERHOG_LOG_LEVEL" |
| <a id="s-d55939d171"></a>`input_shape` | "environment-string" |
| <a id="s-a5c464e916"></a>`name` | "RIVERHOG_LOG_LEVEL" |
| <a id="s-ec108fde99"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-460bc3cffd"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_LOG_LEVEL](../../../evidence/sources.md#src-cb4528636e) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_LOG_LEVEL', DEFAULT_LOG_LEVEL)` |

### Machine authority

- `/external_contract/configuration_environment/63`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1486e6dc0b3d045996c4f025f3ad92c19e74c4501e335d855e314d4196eaf6ad -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "DEFAULT_LOG_LEVEL"
  ],
  "id": "riverhog-server:environment:RIVERHOG_LOG_LEVEL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_LOG_LEVEL",
  "owner": "riverhog-server"
}
```
