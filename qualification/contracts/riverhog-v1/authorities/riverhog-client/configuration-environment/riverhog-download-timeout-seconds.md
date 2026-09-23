# RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-timeout-seconds:357252405d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-702b6e89b0"></a>

| Field | Value |
|---|---|
| <a id="s-80c38a9bb0"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-fcbdf40d24"></a>`default_expressions` | `["unset"]` |
| <a id="s-fe4d544a2c"></a>`id` | `"riverhog-client:environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS"` |
| <a id="s-e0043c7ac0"></a>`input_shape` | `"environment-string"` |
| <a id="s-3bcc5ed990"></a>`name` | `"RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS"` |
| <a id="s-c44d6a61f5"></a>`owner` | `"riverhog-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](#s-702b6e89b0) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4480039bc0"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-3523da0104"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-7d03d4b83f) — [packages/riverhog-client/src/riverhog\_client/client.py::\_timeout\_seconds](../../../../../../packages/riverhog-client/src/riverhog_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/client.py](../../../../../../packages/riverhog-client/src/riverhog_client/client.py) | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/158`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff0559c69365382acfe7ffc1902e0f49f9136e33c6baa6af39f9b95563780f8d -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS",
  "owner": "riverhog-client"
}
```

</details>
