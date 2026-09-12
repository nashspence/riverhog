# RIVERHOG_PROVENANCE_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-provenance:riverhog-provenance-state-home:6957a1ba27 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-0c85a6c495) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eb2021dc66"></a>
| Field | Shape |
|---|---|
| <a id="s-db40d67310"></a>`classification` | "identity" |
| <a id="s-f72318c871"></a>`consumers` | ["riverhog-provenance"] |
| <a id="s-e8e38d1516"></a>`disposition` | "contractual" |
| <a id="s-f6f255e1f5"></a>`id` | "riverhog-provenance:environment:RIVERHOG_PROVENANCE_STATE_HOME" |
| <a id="s-b4738a40ad"></a>`name` | "RIVERHOG_PROVENANCE_STATE_HOME" |
| <a id="s-407a9add94"></a>`owner` | "riverhog-provenance" |

## Governing policies

- <a id="pa-059471dbfc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-provenance:RIVERHOG_PROVENANCE_STATE_HOME](../../../evidence/sources.md#src-9fe04e9d42) — `packages/riverhog-provenance/src/riverhog_provenance/identity.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/11/names` |
| parser | `riverhog-provenance` | `packages/riverhog-provenance/src/riverhog_provenance/identity.py` | `os.getenv('RIVERHOG_PROVENANCE_STATE_HOME')` |

### Machine authority

- `/external_contract/configuration_environment/29`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c14abaa9ff2fb90db19c368e9c4c03b014e6cb24bd0d839d310b206d6d63caa1 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-provenance"
  ],
  "disposition": "contractual",
  "id": "riverhog-provenance:environment:RIVERHOG_PROVENANCE_STATE_HOME",
  "name": "RIVERHOG_PROVENANCE_STATE_HOME",
  "owner": "riverhog-provenance"
}
```
