# gogurt component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt:gogurt-component-boundary:33eef380d6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-002136e1bcbf) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5967c94e653f"></a>
| Field | Shape |
|---|---|
| <a id="s-8d6c788714df"></a>`console_scripts` | additional keys=`gogurt` |
| <a id="s-2dc654e89504"></a>`dependencies` | ["config-validation","gogurt-core","gogurt-listener-runtime"] |
| <a id="s-d37e423edd2c"></a>`distribution` | "gogurt" |
| <a id="s-3456e57da274"></a>`optional_dependencies` | empty object |
| <a id="s-56638e7abf8a"></a>`path` | "reference/gogurt/application" |
| <a id="s-cd3d560b7f57"></a>`role` | "reference_application" |

## Governing policies

- <a id="pa-0cd18f7df096"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/15`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44cf87689308a59b50d1f183145ad8732790c85607a9b3a702f5e463f728105a -->

```json
{
  "console_scripts": {
    "gogurt": "gogurt.cli:main"
  },
  "dependencies": [
    "config-validation",
    "gogurt-core",
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt",
  "optional_dependencies": {},
  "path": "reference/gogurt/application",
  "role": "reference_application"
}
```
