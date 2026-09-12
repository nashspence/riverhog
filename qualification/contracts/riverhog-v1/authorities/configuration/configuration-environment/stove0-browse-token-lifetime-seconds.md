# STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-browse-token-lifetime-seconds:7901637236 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-cbedc559e6f7"></a>
| Field | Shape |
|---|---|
| <a id="s-339b989f89de"></a>`consumers` | ["stove0-server"] |
| <a id="s-7dead87c9e73"></a>`name` | "STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](#s-cbedc559e6f7) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-53f6bb4014b5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-ec48cef35b4d"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../../../evidence/sources.md#src-6490df2f5a1e) — `configuration-environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/82`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2bcf43717792d7bbf30d204093b3a110db6c9a471b35db259d62f5a8b7af2ded -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"
}
```
