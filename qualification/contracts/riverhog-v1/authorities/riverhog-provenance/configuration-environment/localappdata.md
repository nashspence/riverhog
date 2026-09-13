# LOCALAPPDATA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-provenance:localappdata:ccfeea7947 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a9fb2eff34"></a>
| Field | Shape |
|---|---|
| <a id="s-c00038c38f"></a>`consumers` | ["riverhog-provenance"] |
| <a id="s-5600fa3893"></a>`default_expressions` | ["unset"] |
| <a id="s-c9b454920d"></a>`id` | "riverhog-provenance:environment:LOCALAPPDATA" |
| <a id="s-bca3c308a4"></a>`input_shape` | "environment-string" |
| <a id="s-caff9576d7"></a>`name` | "LOCALAPPDATA" |
| <a id="s-69d21c7a53"></a>`owner` | "riverhog-provenance" |

## Governing policies

- <a id="pa-9ed79fe44e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-provenance:LOCALAPPDATA](../../../evidence/sources.md#src-d5c0e21c89) — `packages/riverhog-provenance/src/riverhog_provenance/identity.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-provenance` | `packages/riverhog-provenance/src/riverhog_provenance/identity.py` | `os.getenv('LOCALAPPDATA')` |

### Machine authority

- `/external_contract/configuration_environment/32`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6eecff273ae7c443f5e6bff5d6d2e70bc64b5b3442e25d6a35baec92679b104a -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-provenance:environment:LOCALAPPDATA",
  "input_shape": "environment-string",
  "name": "LOCALAPPDATA",
  "owner": "riverhog-provenance"
}
```
