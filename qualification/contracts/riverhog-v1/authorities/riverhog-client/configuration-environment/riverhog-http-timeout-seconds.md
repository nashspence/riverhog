# RIVERHOG_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-http-timeout-seconds:cf318cd02b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-d9dcf35fac"></a>
| Field | Shape |
|---|---|
| <a id="s-ec39c6e551"></a>`classification` | "runtime" |
| <a id="s-10a1d823b2"></a>`consumers` | ["riverhog-client"] |
| <a id="s-cc44e0ff86"></a>`disposition` | "contractual" |
| <a id="s-4ed1357761"></a>`id` | "riverhog-client:environment:RIVERHOG_HTTP_TIMEOUT_SECONDS" |
| <a id="s-632be535e2"></a>`name` | "RIVERHOG_HTTP_TIMEOUT_SECONDS" |
| <a id="s-0341ad22d1"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_HTTP_TIMEOUT_SECONDS"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_HTTP_TIMEOUT_SECONDS](#s-d9dcf35fac) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-5728db2326"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-392103e194"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_HTTP_TIMEOUT_SECONDS](../../../evidence/sources.md#src-908bba2b9b) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `_timeout_seconds('RIVERHOG_HTTP_TIMEOUT_SECONDS', _HTTP_TIMEOUT_SECONDS)` |

### Machine authority

- `/external_contract/configuration_environment/16`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 460b36d0b800930673d59ec402df8f0224025d46ef46fdfd0365222bce2653d4 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_HTTP_TIMEOUT_SECONDS",
  "name": "RIVERHOG_HTTP_TIMEOUT_SECONDS",
  "owner": "riverhog-client"
}
```
