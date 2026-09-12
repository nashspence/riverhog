# GOGURT_MOUNTED_VOLUME_PROVIDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:gogurt-mounted-volume-provider:706cf99e7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0d1f6f2eed"></a>
| Field | Shape |
|---|---|
| <a id="s-1351b3e99a"></a>`consumers` | ["gogurt"] |
| <a id="s-4e078e6d98"></a>`name` | "GOGURT_MOUNTED_VOLUME_PROVIDER" |

## Governing policies

- <a id="pa-d21c59e1ec"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER](../../../evidence/sources.md#src-3ca721321c) — `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09fd0c5a895533f8f9d7bba28538d2f69f9e685dd837e93dcbc59a861da66b34 -->

```json
{
  "consumers": [
    "gogurt"
  ],
  "name": "GOGURT_MOUNTED_VOLUME_PROVIDER"
}
```
