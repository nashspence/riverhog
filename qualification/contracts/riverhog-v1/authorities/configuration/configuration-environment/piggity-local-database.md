# PIGGITY_LOCAL_DATABASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-local-database:f05c943e01 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b221afcadd"></a>
| Field | Shape |
|---|---|
| <a id="s-81eb745f84"></a>`consumers` | ["piggity"] |
| <a id="s-9bc5608703"></a>`name` | "PIGGITY_LOCAL_DATABASE" |

## Governing policies

- <a id="pa-eead89e04b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:PIGGITY_LOCAL_DATABASE](../../../evidence/sources.md#src-66ec616688) — `configuration-environment:PIGGITY_LOCAL_DATABASE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 272c20ad2c70c99d0905cadbee302b11cbf10260e234bb528f22584ab47d87a5 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_LOCAL_DATABASE"
}
```
