# RIVERHOG_DOWNLOAD_FILE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-file-concurrency:b1d5ecf0df -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ebe5f6e564"></a>

| Field | Value |
|---|---|
| <a id="s-bbc93bda10"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-280060eac6"></a>`default_expressions` | `["''"]` |
| <a id="s-a700ea2fa0"></a>`id` | `"riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"` |
| <a id="s-3c3b66b4ca"></a>`input_shape` | `"environment-string"` |
| <a id="s-4e5ff21edf"></a>`name` | `"RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"` |
| <a id="s-2def1b5bdc"></a>`owner` | `"riverhog-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](#s-ebe5f6e564) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e222239e2b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-153749c279"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../../../evidence/sources/authorities.md#src-bb20193981) — [packages/riverhog-client/src/riverhog\_client/downloads.py::configured\_download\_concurrency](../../../../../../packages/riverhog-client/src/riverhog_client/downloads.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/downloads.py](../../../../../../packages/riverhog-client/src/riverhog_client/downloads.py) | `environment.get('RIVERHOG_DOWNLOAD_FILE_CONCURRENCY', '')` |

### Machine authority

- `/external_contract/configuration_environment/156`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2f649f5e7447801198a948df67f2ccc53d6fd7234e8e8e5f35cd0c6f73260ff -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY",
  "owner": "riverhog-client"
}
```

</details>
