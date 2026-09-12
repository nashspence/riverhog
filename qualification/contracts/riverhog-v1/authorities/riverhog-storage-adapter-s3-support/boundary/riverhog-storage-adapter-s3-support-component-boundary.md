# riverhog-storage-adapter-s3-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-compo-843346b56b:cbc3e1878a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-d33d0c1da8d1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7bc32b5108bb"></a>
| Field | Shape |
|---|---|
| <a id="s-f319d5215441"></a>`console_scripts` | empty object |
| <a id="s-dc98f98850fe"></a>`dependencies` | ["riverhog-storage-adapter-protocol","time-formats"] |
| <a id="s-25809c0c8b6e"></a>`distribution` | "riverhog-storage-adapter-s3-support" |
| <a id="s-181ddfcf7f7f"></a>`optional_dependencies` | empty object |
| <a id="s-5d6b3d52d85f"></a>`path` | "reference/riverhog/storage/s3-support" |
| <a id="s-66bce49f01ab"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-3779b6dbdf21"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/39`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 326d2e4942527c5fbdfb7dc6308303a87688522fe86aa39abb92aa9c9e6ef704 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-storage-adapter-protocol",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-s3-support",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/s3-support",
  "role": "reference_component"
}
```
