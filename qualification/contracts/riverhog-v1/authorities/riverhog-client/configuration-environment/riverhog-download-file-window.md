# RIVERHOG_DOWNLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-file-window:9d0ee02533 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1ebfad79c3"></a>
| Field | Shape |
|---|---|
| <a id="s-42ed9823cb"></a>`consumers` | ["riverhog-client"] |
| <a id="s-9cc68ab77f"></a>`default_expressions` | ["''"] |
| <a id="s-27fcc796bf"></a>`id` | "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_WINDOW" |
| <a id="s-6d9f662f4d"></a>`input_shape` | "environment-string" |
| <a id="s-def2479a70"></a>`name` | "RIVERHOG_DOWNLOAD_FILE_WINDOW" |
| <a id="s-18f1726a77"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_WINDOW"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_WINDOW](#s-1ebfad79c3) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-7cbfc28a4c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-08f6da6ef2"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_WINDOW](../../../evidence/sources.md#src-4629108138) — `packages/riverhog-client/src/riverhog_client/downloads.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/downloads.py` | `environment.get('RIVERHOG_DOWNLOAD_FILE_WINDOW', '')` |

### Machine authority

- `/external_contract/configuration_environment/15`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 167f8e64419b3d5e01313381e02181e247300df4588834df1ce9dda4b6b013b9 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_WINDOW",
  "input_shape": "environment-string",
  "name": "RIVERHOG_DOWNLOAD_FILE_WINDOW",
  "owner": "riverhog-client"
}
```
