# RIVERHOG_PUBLIC_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-public-base-url:0ed641ed6d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-96459587f4"></a>
| Field | Shape |
|---|---|
| <a id="s-77d8284e62"></a>`consumers` | ["riverhog-server"] |
| <a id="s-a4e0febe13"></a>`name` | "RIVERHOG_PUBLIC_BASE_URL" |

## Governing policies

- <a id="pa-7fdb30a26d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_PUBLIC_BASE_URL](../../../evidence/sources.md#src-d5b1b6177d) — `configuration-environment:RIVERHOG_PUBLIC_BASE_URL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/56`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1416d7a583c2ee256c77e25eadf1c655bb5a3c695c2b01442c0b923278a1275d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_PUBLIC_BASE_URL"
}
```
