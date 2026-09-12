# gogurt-macos-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-macos-mounted-volume:gogurt-macos-mounted-volume-component-boundary:b842309763 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-mounted-volume](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-837b7b4fff56) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7c157a5482c1"></a>
| Field | Shape |
|---|---|
| <a id="s-540934963fea"></a>`console_scripts` | empty object |
| <a id="s-f6def86636ea"></a>`dependencies` | ["gogurt-core","gogurt-path-volume-support"] |
| <a id="s-de3eaf91eaa8"></a>`distribution` | "gogurt-macos-mounted-volume" |
| <a id="s-43a8cf7982e8"></a>`optional_dependencies` | empty object |
| <a id="s-5a45bb4bb6d1"></a>`path` | "reference/gogurt/mounted-volume/macos" |
| <a id="s-b30bbe70dac7"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-7083ffe7620f"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9640e3d7a5bac1acb70c260630c215d11815fafa1b182477da0157ed68671ef -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-core",
    "gogurt-path-volume-support"
  ],
  "distribution": "gogurt-macos-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/macos",
  "role": "reference_component"
}
```
