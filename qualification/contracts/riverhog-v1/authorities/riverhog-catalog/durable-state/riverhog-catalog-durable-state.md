# riverhog-catalog durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-durable-state:e07af31f57 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [durable-state](index.md) |
| Family | [owners](index.md#f-d362e5b150d1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0f3e6e400804"></a>
- <a id="s-5781e0d37665"></a>`format`: state-schema/postgresql

## Governing policies

- <a id="pa-22f80ed6bf02"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51ec6)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a1467008) — `state:riverhog-catalog`

### Machine authority

- `/external_contract/durable_state/owners/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6f90a3b0c440c77926b608ea9007e4d5153484f7a506f5eaac7660fa8166e08 -->

```json
{
  "distribution": "riverhog-server",
  "fixture_sha256s": [
    "8b337f69f6bdc2665afd1b24a645301878925997d1a4416b607c82204d68bc96"
  ],
  "format": "state-schema/postgresql",
  "head": "v1_0001",
  "id": "riverhog-catalog"
}
```
