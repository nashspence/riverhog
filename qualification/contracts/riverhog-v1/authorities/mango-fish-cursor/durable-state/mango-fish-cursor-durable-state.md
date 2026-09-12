# mango-fish-cursor durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:mango-fish-cursor:mango-fish-cursor-durable-state:2504b0617b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish-cursor](../index.md) |
| Interface | [durable-state](index.md) |
| Family | [owners](index.md#f-ffc7e6b5c3) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cb3dcc8c5c"></a>
- <a id="s-0773785869"></a>`format`: state-schema/sqlite

## Governing policies

- <a id="pa-2f33697d31"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:mango-fish-cursor](../../../evidence/sources.md#src-b1cc215b8d) — `state:mango-fish-cursor`

### Machine authority

- `/external_contract/durable_state/owners/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 957ea6c69fd3f899e5f9f0053f31445028a07f80bc240b34fbf70797df14b586 -->

```json
{
  "distribution": "mango-fish",
  "fixture_sha256s": [
    "f1baf4752b190b555de143a53a7b68eb41785895fa1d353c86e90b111fa6bf96"
  ],
  "format": "state-schema/sqlite",
  "head": "v1_0001",
  "id": "mango-fish-cursor"
}
```
