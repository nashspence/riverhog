# RIVERHOG_BOOTSTRAP_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-bootstrap-token:80f671d884 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](families/credential/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c80b060880"></a>
| Field | Shape |
|---|---|
| <a id="s-71ddf8c813"></a>`classification` | "credential" |
| <a id="s-fe8922d859"></a>`consumers` | ["riverhog-server"] |
| <a id="s-0af1ebd957"></a>`disposition` | "contractual" |
| <a id="s-8546fde067"></a>`id` | "riverhog-server:environment:RIVERHOG_BOOTSTRAP_TOKEN" |
| <a id="s-e9729640af"></a>`name` | "RIVERHOG_BOOTSTRAP_TOKEN" |
| <a id="s-09d7007204"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-698bac0d51"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_BOOTSTRAP_TOKEN](../../../evidence/sources.md#src-46bdbba342) — `riverhog/src/riverhog_api/auth.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/12/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_api/auth.py` | `os.getenv(BOOTSTRAP_TOKEN_ENV, '')` |

### Machine authority

- `/external_contract/configuration_environment/43`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bae185c0fda4ebaa8a9cd089fc59edb3ab03792383b48d92d097d5a17fe733b -->

```json
{
  "classification": "credential",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_BOOTSTRAP_TOKEN",
  "name": "RIVERHOG_BOOTSTRAP_TOKEN",
  "owner": "riverhog-server"
}
```
