# RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-active-passphrase-id:3e56f3cb4d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ee8d4c5bb2"></a>
| Field | Shape |
|---|---|
| <a id="s-9cef198e9c"></a>`consumers` | ["riverhog-server"] |
| <a id="s-4e914a87ae"></a>`default_expressions` | ["''"] |
| <a id="s-c87995acf3"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID" |
| <a id="s-1c5840c446"></a>`input_shape` | "environment-string" |
| <a id="s-199cea8e22"></a>`name` | "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID" |
| <a id="s-a35e48e93a"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-8190e92e05"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID](../../../evidence/sources.md#src-59ce1b7699) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID', '')` |

### Machine authority

- `/external_contract/configuration_environment/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d0eef78a4476706a6cea21a19d50dd065e64dfee3dd42da86992b49a25d8a8d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID",
  "owner": "riverhog-server"
}
```
