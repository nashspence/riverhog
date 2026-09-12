# riverhog-recover component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-recover:riverhog-recover-component-boundary:23d8e1f86c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-40d58069da) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eef4f8f92a"></a>
| Field | Shape |
|---|---|
| <a id="s-97d1a7c66a"></a>`console_scripts` | additional keys=`riverhog-recover` |
| <a id="s-8124681543"></a>`dependencies` | ["riverhog-archive-contracts","riverhog-protocol","riverhog-provenance"] |
| <a id="s-47852cec70"></a>`distribution` | "riverhog-recover" |
| <a id="s-9c11753f3d"></a>`optional_dependencies` | empty object |
| <a id="s-bee0d93ac6"></a>`path` | "reference/riverhog/recovery" |
| <a id="s-0da1e7ba7e"></a>`role` | "reference_application" |

## Governing policies

- <a id="pa-e92ccc51ee"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/35`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbf685bdb666a92c0942e8c92d1e82b4c6bc3811bad73b2808a9be283205053f -->

```json
{
  "console_scripts": {
    "riverhog-recover": "riverhog_recover.cli:main"
  },
  "dependencies": [
    "riverhog-archive-contracts",
    "riverhog-protocol",
    "riverhog-provenance"
  ],
  "distribution": "riverhog-recover",
  "optional_dependencies": {},
  "path": "reference/riverhog/recovery",
  "role": "reference_application"
}
```
