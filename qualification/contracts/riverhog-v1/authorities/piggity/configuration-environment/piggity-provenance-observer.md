# PIGGITY_PROVENANCE_OBSERVER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-provenance-observer:531245b93d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-5718a6415e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-38bec3b929"></a>
| Field | Shape |
|---|---|
| <a id="s-1f74ee3e3d"></a>`classification` | "identity" |
| <a id="s-9924516b14"></a>`consumers` | ["piggity"] |
| <a id="s-af05d8f667"></a>`disposition` | "contractual" |
| <a id="s-1fe06e52b2"></a>`id` | "piggity:environment:PIGGITY_PROVENANCE_OBSERVER" |
| <a id="s-a68f317075"></a>`name` | "PIGGITY_PROVENANCE_OBSERVER" |
| <a id="s-db30e5af06"></a>`owner` | "piggity" |

## Governing policies

- <a id="pa-982d7120c1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:piggity:PIGGITY_PROVENANCE_OBSERVER](../../../evidence/sources.md#src-df0bfb7f70) — `reference/riverhog/applications/piggity/src/piggity/main.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/1/names` |
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/main.py` | `typer.Option('--provenance-observer', envvar='PIGGITY_PROVENANCE_OBSERVER', help='Explicit installed provenance observer provider name')` |

### Machine authority

- `/external_contract/configuration_environment/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96d666c09ff168a96c7d82850062df77ae916418e9d3c6b30d3225033ba08d8e -->

```json
{
  "classification": "identity",
  "consumers": [
    "piggity"
  ],
  "disposition": "contractual",
  "id": "piggity:environment:PIGGITY_PROVENANCE_OBSERVER",
  "name": "PIGGITY_PROVENANCE_OBSERVER",
  "owner": "piggity"
}
```
