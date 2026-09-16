# RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-collection-upload-custody-lease:e884d856c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-96459587f4"></a>

| Field | Value |
|---|---|
| <a id="s-77d8284e62"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-a4ea8ca34c"></a>`default_expressions` | `["'1h'"]` |
| <a id="s-db2f70ffc3"></a>`id` | `"riverhog-server:environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"` |
| <a id="s-f6de587738"></a>`input_shape` | `"environment-string"` |
| <a id="s-a4e0febe13"></a>`name` | `"RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"` |
| <a id="s-efcd10c3b8"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](#s-96459587f4) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-0ece5057b9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-4ee780e91c"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../../../evidence/sources.md#src-4cfc3a8358) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE', '1h')` |

### Machine authority

- `/external_contract/configuration_environment/56`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77d803267c8b64aaeb043d8471cdef294f499101f9b04d4dac2fd630ed25d49f -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'1h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE",
  "owner": "riverhog-server"
}
```

</details>
