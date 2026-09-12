# configuration_environment_patterns-0

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:configuration-environment-patterns-0:686eafb0a8 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `patterns` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/configuration_environment_patterns/0`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:inventory` — `scripts/contract_freeze.py::_environment_inventory`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Contract summary

| Field | Shape |
|---|---|
| `consumer` | "riverhog-server" |
| `parameters` | object (2 fields) |
| `template` | "RIVERHOG_ARCHIVE_STORE_{store}_{setting}" |

## Complete owned contract

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
