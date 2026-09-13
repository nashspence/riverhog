# RIVERHOG_UPLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-upload-timeout-seconds:4f29822b62 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-b211a29ced"></a>
| Field | Shape |
|---|---|
| <a id="s-9c278af545"></a>`consumers` | ["riverhog-client"] |
| <a id="s-ccc7b8f4c0"></a>`default_expressions` | ["unset"] |
| <a id="s-d0c4986063"></a>`id` | "riverhog-client:environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS" |
| <a id="s-8f8f263604"></a>`input_shape` | "environment-string" |
| <a id="s-18bbe5ebd0"></a>`name` | "RIVERHOG_UPLOAD_TIMEOUT_SECONDS" |
| <a id="s-2f1faef8a9"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_TIMEOUT_SECONDS"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_TIMEOUT_SECONDS](#s-b211a29ced) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-a87f3e6644"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-0839ec441a"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../../../evidence/sources.md#src-6e31cc159d) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/23`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 70f539f9c6e4233625911bd5affb179f33472c98714b949769190a93618d0482 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_UPLOAD_TIMEOUT_SECONDS",
  "owner": "riverhog-client"
}
```
