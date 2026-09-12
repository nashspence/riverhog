# RIVERHOG_RETRIEVAL_MAX_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-max-lease:95112c89a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-90cd77baf72d"></a>
| Field | Shape |
|---|---|
| <a id="s-bc48d55b1e06"></a>`consumers` | ["riverhog-server"] |
| <a id="s-35c021851e63"></a>`name` | "RIVERHOG_RETRIEVAL_MAX_LEASE" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_LEASE"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_LEASE](#s-90cd77baf72d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4be724f93e29"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-7769c853a614"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE](../../../evidence/sources.md#src-c8d096f38ce8) — `configuration-environment:RIVERHOG_RETRIEVAL_MAX_LEASE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/66`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb221e77dc91f04d9e0cba071f0693f01f35b5a97dbb5213df906e5ccb44a791 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_MAX_LEASE"
}
```
