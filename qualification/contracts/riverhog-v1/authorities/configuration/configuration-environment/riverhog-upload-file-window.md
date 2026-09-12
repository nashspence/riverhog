# RIVERHOG_UPLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-upload-file-window:e7d78d93de -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-0e8ce1fd7c"></a>
| Field | Shape |
|---|---|
| <a id="s-8e543c475e"></a>`consumers` | ["riverhog-client"] |
| <a id="s-38ce56b288"></a>`name` | "RIVERHOG_UPLOAD_FILE_WINDOW" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_FILE_WINDOW"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_FILE_WINDOW](#s-0e8ce1fd7c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-c46f567925"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-7585230b3f"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW](../../../evidence/sources.md#src-c97645ded2) — `configuration-environment:RIVERHOG_UPLOAD_FILE_WINDOW`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/76`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ca5e777359c7a0f653db5b30731570c3eca4066696eef65dd566e20fea95918 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_UPLOAD_FILE_WINDOW"
}
```
