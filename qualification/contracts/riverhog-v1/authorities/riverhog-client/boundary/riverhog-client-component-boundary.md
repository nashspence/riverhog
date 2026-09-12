# riverhog-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-client:riverhog-client-component-boundary:ad6c59ed6b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-467493217f) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5178530295"></a>
| Field | Shape |
|---|---|
| <a id="s-3ffa683045"></a>`console_scripts` | empty object |
| <a id="s-f1e77bae11"></a>`dependencies` | ["http-api-contracts","riverhog-application-access","riverhog-protocol","riverhog-provenance-contracts"] |
| <a id="s-5a95a76d76"></a>`distribution` | "riverhog-client" |
| <a id="s-230ffe877c"></a>`optional_dependencies` | empty object |
| <a id="s-e0dad484a4"></a>`path` | "packages/riverhog-client" |
| <a id="s-12071d4f22"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-6fe48bfd21"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e178deea5a616eb3c3dfe0ca6cce73d0c3de381d54649d383895f27e2249bcca -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-application-access",
    "riverhog-protocol",
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-client",
  "optional_dependencies": {},
  "path": "packages/riverhog-client",
  "role": "reusable_library"
}
```
