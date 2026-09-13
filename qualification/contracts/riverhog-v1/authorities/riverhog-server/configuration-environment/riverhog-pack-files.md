# RIVERHOG_PACK_FILES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-files:da8acaa991 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1c368e3bbb"></a>
| Field | Shape |
|---|---|
| <a id="s-0e717d7f5c"></a>`consumers` | ["riverhog-server"] |
| <a id="s-ed83d1071c"></a>`default_expressions` | ["unset"] |
| <a id="s-8acc408344"></a>`id` | "riverhog-server:environment:RIVERHOG_PACK_FILES" |
| <a id="s-18b068f2a0"></a>`input_shape` | "environment-string" |
| <a id="s-561f90128c"></a>`name` | "RIVERHOG_PACK_FILES" |
| <a id="s-2750bfabee"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-02ef5691ff"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_PACK_FILES](../../../evidence/sources.md#src-7d59fe1eef) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/64`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a1373931e59e554060969427f31f1370f093cab111f0454e3670e8eb92b36e9 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_PACK_FILES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_PACK_FILES",
  "owner": "riverhog-server"
}
```
