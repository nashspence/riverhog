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

<a id="s-b221afcaddd5"></a>
| Field | Shape |
|---|---|
| <a id="s-81eb745f84ca"></a>`consumers` | ["piggity"] |
| <a id="s-9bc5608703bd"></a>`name` | "PIGGITY_LOCAL_DATABASE" |

## Governing policies

- <a id="pa-eead89e04b2e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:PIGGITY_LOCAL_DATABASE](../../../evidence/sources.md#src-66ec61668844) — `configuration-environment:PIGGITY_LOCAL_DATABASE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

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
