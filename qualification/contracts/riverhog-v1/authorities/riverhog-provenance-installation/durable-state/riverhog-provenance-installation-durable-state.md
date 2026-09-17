# riverhog-provenance-installation durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-provenance-installation:riverhog-provenance-installation-durable-state:2a3ee53941 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-installation](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-4250c856b8"></a>



| Field | Value |
|---|---|
| <a id="s-20c7aa4825"></a>`distribution` | `"riverhog-provenance"` |
| <a id="s-0ce078a71d"></a>`format` | `"riverhog-provenance-installation-id/v1"` |
| <a id="s-4b5b4b7d11"></a>`head` | `"v1"` |
| <a id="s-d6a8830309"></a>`id` | `"riverhog-provenance-installation"` |
| <a id="s-c1a86443e6"></a>`structure · encoding` | `"ascii"` |
| <a id="s-9ed44fc954"></a>`structure · kind` | `"text-document"` |
| <a id="s-4ab3cff37a"></a>`structure · line_count` | `1` |
| <a id="s-f258d7bd79"></a>`structure · terminator` | `"LF"` |
| <a id="s-fda4dadc04"></a>`structure · value · kind` | `"canonical-uuid-urn"` |
| <a id="s-f8abb85d19"></a>`structure · value · pattern` | `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"` |
| <a id="s-4ee2fb76e5"></a>`transition` | `"immutable-identity"` |

## Governing policies

- <a id="pa-7845ed3800"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-provenance-installation](../../../evidence/sources.md#src-080b970190) — [packages/riverhog-provenance/src/riverhog\_provenance/identity.py::\_installation\_id\_state\_contract](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py)

### Machine authority

- `/external_contract/durable_state/owners/7`

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
