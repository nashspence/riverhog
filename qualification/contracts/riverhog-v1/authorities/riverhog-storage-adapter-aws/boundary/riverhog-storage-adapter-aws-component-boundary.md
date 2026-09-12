# riverhog-storage-adapter-aws component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-component-boundary:41c3df727d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-8e72b87d38d6) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a04fc61b1af9"></a>
| Field | Shape |
|---|---|
| <a id="s-4d92a92098d8"></a>`console_scripts` | additional keys=`riverhog-storage-adapter-aws` |
| <a id="s-dbdc57bec45c"></a>`dependencies` | ["riverhog-storage-adapter-asgi-support","riverhog-storage-adapter-protocol","riverhog-storage-adapter-s3-support","time-formats"] |
| <a id="s-ff6684cce6a8"></a>`distribution` | "riverhog-storage-adapter-aws" |
| <a id="s-57766247befb"></a>`optional_dependencies` | empty object |
| <a id="s-3b85247de86f"></a>`path` | "reference/riverhog/storage/aws" |
| <a id="s-991e6fb7a50d"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-1becb11fcfb3"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/36`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 525b75852f9bcc4c78386c910df506a9d41e9dd5dd31c02c1539907405848683 -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-aws": "riverhog_storage_adapter_aws.app:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-s3-support",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-aws",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/aws",
  "role": "reference_component"
}
```
