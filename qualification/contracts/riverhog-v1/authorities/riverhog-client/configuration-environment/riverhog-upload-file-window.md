# RIVERHOG_UPLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-upload-file-window:01e7371bb8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-f3c5c446af"></a>
| Field | Shape |
|---|---|
| <a id="s-e5f41d64e9"></a>`consumers` | ["riverhog-client"] |
| <a id="s-ee3ab728a9"></a>`default_expressions` | ["''"] |
| <a id="s-4f2d62dbc3"></a>`id` | "riverhog-client:environment:RIVERHOG_UPLOAD_FILE_WINDOW" |
| <a id="s-63e22c344b"></a>`input_shape` | "environment-string" |
| <a id="s-034b439c45"></a>`name` | "RIVERHOG_UPLOAD_FILE_WINDOW" |
| <a id="s-5b255b15bc"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_FILE_WINDOW"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_FILE_WINDOW](#s-f3c5c446af) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-bc7da37588"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-ede95780ea"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_WINDOW](../../../evidence/sources.md#src-93e4082935) — `packages/riverhog-client/src/riverhog_client/uploads.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/uploads.py` | `environment.get('RIVERHOG_UPLOAD_FILE_WINDOW', '')` |

### Machine authority

- `/external_contract/configuration_environment/22`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc22d3a4233a02d4708af7eefab7164674a5f35e44dadc157673270ff03f8e52 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_UPLOAD_FILE_WINDOW",
  "input_shape": "environment-string",
  "name": "RIVERHOG_UPLOAD_FILE_WINDOW",
  "owner": "riverhog-client"
}
```
