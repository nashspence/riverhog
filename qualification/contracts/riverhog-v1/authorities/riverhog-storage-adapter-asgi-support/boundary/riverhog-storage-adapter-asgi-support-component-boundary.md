# riverhog-storage-adapter-asgi-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support-com-c5256e8bd2:1a3ef4052a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-asgi-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-fad3ccd21e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6aabe3af3c"></a>
| Field | Shape |
|---|---|
| <a id="s-ae978f5807"></a>`console_scripts` | empty object |
| <a id="s-5f2b1fe90f"></a>`dependencies` | ["http-api-contracts","riverhog-storage-adapter-protocol","riverhog-storage-adapter-support"] |
| <a id="s-bee601b402"></a>`distribution` | "riverhog-storage-adapter-asgi-support" |
| <a id="s-bd12e153e4"></a>`optional_dependencies` | empty object |
| <a id="s-0af0fa1ade"></a>`path` | "packages/riverhog-storage-adapter-asgi-support" |
| <a id="s-84636dca1f"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-6419ae1aa9"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/10`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 258b4b5cdeb0997fb68d8c095c0f514cc7e7a5374fd79944b3e85f5593a52204 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-support"
  ],
  "distribution": "riverhog-storage-adapter-asgi-support",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-asgi-support",
  "role": "reusable_library"
}
```
