# piggity-local durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-durable-state:570c36bae9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity-local](../index.md) |
| Interface | [durable-state](index.md) |
| Family | [owners](index.md#f-1b3ca97aea1e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5e20eea672e2"></a>
- <a id="s-48380901bc61"></a>`format`: state-schema/sqlite

## Governing policies

- <a id="pa-e09cc0fa63e0"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51ec6)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [state:piggity-local](../../../evidence/sources.md#src-f6a1289f6703) — `state:piggity-local`

### Machine authority

- `/external_contract/durable_state/owners/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b61cb79a35d11a1b633bb803b5164736004af4cb0cee4b83e92530fa36f697e3 -->

```json
{
  "distribution": "piggity",
  "fixture_sha256s": [
    "f848ff7767d1fe3c70b8194c7b9334aaccf2f3c3ae26e3af99b48c86faf87a7f"
  ],
  "format": "state-schema/sqlite",
  "head": "v1_0001",
  "id": "piggity-local"
}
```
