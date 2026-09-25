# riverhog-provenance-installation durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-provenance-installation:riverhog-provenance-installation-durable-state:32016c829d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-installation](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-63882dcd71"></a>



| Field | Value |
|---|---|
| <a id="s-01971875cd"></a>`distribution` | `"riverhog-provenance"` |
| <a id="s-6bdb379175"></a>`format` | `"riverhog-provenance-installation-id/v1"` |
| <a id="s-577a9bba69"></a>`head` | `"v1"` |
| <a id="s-dd8678d748"></a>`id` | `"riverhog-provenance-installation"` |
| <a id="s-47c4855ae8"></a>`structure · encoding` | `"ascii"` |
| <a id="s-ee2868a530"></a>`structure · kind` | `"text-document"` |
| <a id="s-7fbca7c0ae"></a>`structure · line_count` | `1` |
| <a id="s-3dd734b0e1"></a>`structure · terminator` | `"LF"` |
| <a id="s-96f571d3d3"></a>`structure · value · kind` | `"canonical-uuid-urn"` |
| <a id="s-1d0ed77989"></a>`structure · value · pattern` | `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"` |
| <a id="s-ab4d72fdce"></a>`transition` | `"immutable-identity"` |

## Governing policies

- <a id="pa-60dc0c6ac3"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-provenance-installation](../../../evidence/sources/authorities.md#src-080b970190) — [packages/riverhog-provenance/src/riverhog\_provenance/identity.py::\_installation\_id\_state\_contract](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py)

### Machine authority

- `/external_contract/durable_state/owners/9`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a75d387c9cce36fc0bc7c58180b70e129a2e5987ba72214ed4b9de20110148c1 -->

```json
{
  "distribution": "riverhog-provenance",
  "format": "riverhog-provenance-installation-id/v1",
  "head": "v1",
  "id": "riverhog-provenance-installation",
  "structure": {
    "encoding": "ascii",
    "kind": "text-document",
    "line_count": 1,
    "terminator": "LF",
    "value": {
      "kind": "canonical-uuid-urn",
      "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    }
  },
  "transition": "immutable-identity"
}
```

</details>
