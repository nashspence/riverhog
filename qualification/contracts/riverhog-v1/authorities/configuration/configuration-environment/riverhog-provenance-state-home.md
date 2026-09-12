# RIVERHOG_PROVENANCE_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-provenance-state-home:cdf1d02b3d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-021597da78d8"></a>
| Field | Shape |
|---|---|
| <a id="s-de450a94081d"></a>`consumers` | ["riverhog-provenance"] |
| <a id="s-9d016ce2fadc"></a>`name` | "RIVERHOG_PROVENANCE_STATE_HOME" |

## Governing policies

- <a id="pa-80de0c8308c8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME](../../../evidence/sources.md#src-6026192e04a4) — `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34353d7c2d47daaacfa034b08c1b2a693b0b397a3352533bf4e136d4f8ba48b3 -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "name": "RIVERHOG_PROVENANCE_STATE_HOME"
}
```
