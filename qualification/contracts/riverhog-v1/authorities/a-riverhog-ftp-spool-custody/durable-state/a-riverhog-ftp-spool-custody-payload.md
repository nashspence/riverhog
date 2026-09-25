# a-riverhog-ftp-spool-custody: payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-ftp-spool-custody:a-riverhog-ftp-spool-custody-payload:0c976dfbb4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-72eb037d2c"></a>



| Field | Value |
|---|---|
| <a id="s-34d3fea01b"></a>`id` | `"payload"` |
| <a id="s-fd8092e924"></a>`identity` | `"claim file path, byte length, and SHA-256"` |
| <a id="s-a6b5245403"></a>`kind` | `"opaque-bytes"` |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-ftp-spool-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-2a01d11ad7"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-ftp-spool-custody](../../../evidence/sources/authorities.md#src-97232522a5) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/8/structure/units/4`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f5427ec521e220cf3cdc02bacbaff164b520f6f610ed506b6a37b9bc078ff2c -->

```json
{
  "id": "payload",
  "identity": "claim file path, byte length, and SHA-256",
  "kind": "opaque-bytes"
}
```

</details>
