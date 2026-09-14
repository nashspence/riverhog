# riverhog-ftp-custody: payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-payload:301bdc2098 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-ebd9b486a9"></a>

| Field | Shape |
|---|---|
| <a id="s-1be383ed84"></a>`id` | "payload" |
| <a id="s-8061ff2e68"></a>`identity` | "claim file path, byte length, and SHA-256" |
| <a id="s-5d95aebccb"></a>`kind` | "opaque-bytes" |

## Maintained corroboration

### Related interface records

- [riverhog-ftp-custody durable-state identity](riverhog-ftp-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-eaccf80426"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-ftp-custody](../../../evidence/sources.md#src-54f88a3a47) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py`

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f5427ec521e220cf3cdc02bacbaff164b520f6f610ed506b6a37b9bc078ff2c -->

```json
{
  "id": "payload",
  "identity": "claim file path, byte length, and SHA-256",
  "kind": "opaque-bytes"
}
```
