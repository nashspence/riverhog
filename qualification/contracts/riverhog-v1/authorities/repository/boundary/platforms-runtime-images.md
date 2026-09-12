# Platforms runtime images

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:platforms-runtime-images:85d134374a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [repository](../index.md) |
| Interface | [boundary](index.md) |
| Family | [runtime-images](index.md#f-dfd50c0c8689) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-910361da52f9"></a>
- Shape: ["linux/amd64"]

## Governing policies

- <a id="pa-84b076b92fb5"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/runtime_images/platforms`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac8335d2e189797f6099e4d37b3fd5a69dcc42bfb5a2b077bd758e7de971ca62 -->

```json
[
  "linux/amd64"
]
```
