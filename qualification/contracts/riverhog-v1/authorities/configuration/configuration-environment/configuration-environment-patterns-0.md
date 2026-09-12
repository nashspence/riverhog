# configuration_environment_patterns-0

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:configuration-environment-patterns-0:686eafb0a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `patterns` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

| Field | Shape |
|---|---|
| `consumer` | "riverhog-server" |
| `parameters` | additional keys=`setting`, `store` |
| `template` | "RIVERHOG_ARCHIVE_STORE_{store}_{setting}" |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Governing policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:inventory` — `scripts/contract_freeze.py::_environment_inventory`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment_patterns/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f14b45de57e080aadf202acdd7c5f105784c253ba0f80302df2b4893b01ede6 -->

```json
{
  "consumer": "riverhog-server",
  "parameters": {
    "setting": [
      "ADAPTER_URL",
      "ADAPTER_TOKEN_FILE",
      "ADAPTER_ALLOW_INSECURE_HTTP",
      "ADAPTER_MAX_CONNECTIONS",
      "ADAPTER_TIMEOUT_SECONDS",
      "MONTHLY_DOWNLOAD_ALLOWANCE_BYTES",
      "DOWNLOAD_SAFETY_BUFFER_BYTES"
    ],
    "store": {
      "normalization": "uppercase-dashes-to-underscores",
      "source": "RIVERHOG_ARCHIVE_STORES"
    }
  },
  "template": "RIVERHOG_ARCHIVE_STORE_{store}_{setting}"
}
```
