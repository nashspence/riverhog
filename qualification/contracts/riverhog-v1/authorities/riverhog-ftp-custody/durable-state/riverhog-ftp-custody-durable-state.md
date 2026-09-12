# riverhog-ftp-custody durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-durable-state:319b9df697 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [durable-state](index.md) |
| Family | [owners](index.md#f-5917b0b920) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a015c4ca4d"></a>
- <a id="s-45f323bd69"></a>`format`: riverhog-ftp-adapter-claim/v1

## Governing policies

- <a id="pa-c738b31b27"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-ftp-custody](../../../evidence/sources.md#src-54f88a3a47) — `state:riverhog-ftp-custody`

### Machine authority

- `/external_contract/durable_state/owners/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1cdcddcc2603b2a8b9a3d31106e7a4f808cf1c62f3ff7a5e45b180e9deee710 -->

```json
{
  "distribution": "riverhog-ftp-adapter",
  "fixture_sha256s": [
    "4ade71a24a784d4893d8f447fedecbab4cdde5257d21e174b2ccd654027be95f",
    "565b24bc77ebeee74f70f6c608e099956666c3589ed85146fcea7e77d9f25356"
  ],
  "format": "riverhog-ftp-adapter-claim/v1",
  "head": "v1",
  "id": "riverhog-ftp-custody"
}
```
