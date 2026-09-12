# RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-request-concurrency:e1d2fe610d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-abc6c0ac51"></a>
| Field | Shape |
|---|---|
| <a id="s-1ed351fe80"></a>`consumers` | ["riverhog-server"] |
| <a id="s-b3f8b0388d"></a>`name` | "RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](#s-abc6c0ac51) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-f4dbb332d5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-77038388fe"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../../../evidence/sources.md#src-5712c8aa59) — `configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/72`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f716c7798dcb781ac44afb906405ecb7f361e4406cca8c3ce3872b6d81a0d723 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"
}
```
