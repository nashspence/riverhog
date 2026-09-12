# riverhog-storage-adapter-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-component-boundary:161627301d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-7cae95a19acb) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7af40162436d"></a>
| Field | Shape |
|---|---|
| <a id="s-bc459187aac2"></a>`console_scripts` | empty object |
| <a id="s-64f17cc16166"></a>`dependencies` | ["time-formats"] |
| <a id="s-34e07144414e"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-45ad5bf7fc0a"></a>`optional_dependencies` | empty object |
| <a id="s-6b5eccc6a47c"></a>`path` | "packages/riverhog-storage-adapter-protocol" |
| <a id="s-363c0d2c273d"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-8e6c6864ec06"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/11`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 632e07c9f235fe41e4baaed295768b2c99f0e8a27458437b82f4838a7767afb9 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-protocol",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-protocol",
  "role": "reusable_library"
}
```
