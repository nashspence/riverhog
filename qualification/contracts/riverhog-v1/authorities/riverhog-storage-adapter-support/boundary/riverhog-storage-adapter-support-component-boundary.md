# riverhog-storage-adapter-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-support:riverhog-storage-adapter-support-component-boundary:0d1479bc2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-41d371cc3faf) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a8933498d677"></a>
| Field | Shape |
|---|---|
| <a id="s-6b826301c967"></a>`console_scripts` | additional keys=`riverhog-storage-adapter-conformance`, `riverhog-storage-adapter-schemas` |
| <a id="s-ddc0830bb7a4"></a>`dependencies` | ["http-api-contracts","riverhog-storage-adapter-protocol"] |
| <a id="s-6685240eb299"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-a4de864bfff4"></a>`optional_dependencies` | empty object |
| <a id="s-c97e539de597"></a>`path` | "packages/riverhog-storage-adapter-support" |
| <a id="s-b94af1eba7e3"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-90aa02ffe58d"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/12`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ffe6aa156e5a9ba0b05a14a363d5fc2dd97e7f9b4a2846d53344cff124364b1 -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-conformance": "riverhog_storage_adapter_support.conformance:main",
    "riverhog-storage-adapter-schemas": "riverhog_storage_adapter_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-storage-adapter-protocol"
  ],
  "distribution": "riverhog-storage-adapter-support",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-support",
  "role": "reusable_library"
}
```
