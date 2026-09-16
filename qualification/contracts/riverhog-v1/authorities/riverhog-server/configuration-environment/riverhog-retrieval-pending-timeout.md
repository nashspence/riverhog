# RIVERHOG_RETRIEVAL_PENDING_TIMEOUT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-pending-timeout:d0f99a0685 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d42d9004a6"></a>

| Field | Value |
|---|---|
| <a id="s-ef9d7442ff"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-aecbad9c35"></a>`default_expressions` | `["'72h'"]` |
| <a id="s-7b216e5267"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT"` |
| <a id="s-ce46db95b3"></a>`input_shape` | `"environment-string"` |
| <a id="s-c980de4ee4"></a>`name` | `"RIVERHOG_RETRIEVAL_PENDING_TIMEOUT"` |
| <a id="s-a9e838bc8b"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_PENDING_TIMEOUT"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](#s-d42d9004a6) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-8e97a303fa"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-635bdb1035"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../../../evidence/sources.md#src-01bd40bbb3) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_PENDING_TIMEOUT', '72h')` |

### Machine authority

- `/external_contract/configuration_environment/79`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13d6f5c3c60e7a02158c9148573e0ee71f192f86057f8190ae9828a4b5f30236 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'72h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_PENDING_TIMEOUT",
  "owner": "riverhog-server"
}
```

</details>
