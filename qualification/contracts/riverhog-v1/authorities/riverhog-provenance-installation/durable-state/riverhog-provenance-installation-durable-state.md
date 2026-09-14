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

- <a id="s-0ce078a71d"></a>`format`: riverhog-provenance-installation-id/v1

## Governing policies

- <a id="pa-7845ed3800"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-provenance-installation](../../../evidence/sources.md#src-080b970190) — `packages/riverhog-provenance/src/riverhog_provenance/identity.py`

### Machine authority

- `/external_contract/durable_state/owners/7`

### Exact owned JSON

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
