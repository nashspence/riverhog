# riverhog.provenance-observers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance:riverhog-provenance-observers:ec9ab0b122 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [boundary](index.md) |
| Family | [entry-point-extensions](index.md#f-39eb2e210fbf) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8e80dc83653f"></a>
| Field | Shape |
|---|---|
| <a id="s-cfcb698762b4"></a>`group` | "riverhog.provenance-observers" |
| <a id="s-c97c7ce482fa"></a>`owner` | "riverhog-provenance" |
| <a id="s-68f6edaf5204"></a>`owner_constant` | "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP" |
| <a id="s-cba32b9784be"></a>`owner_path` | "packages/riverhog-provenance" |
| <a id="s-ccc7c348ff42"></a>`providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

## Governing policies

- <a id="pa-f8d8e88cd631"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/entry_point_extensions/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f3d605d20af9e4414e252363e434d4c804835100ca9a69af7d5ac9b9bafa061 -->

```json
{
  "group": "riverhog.provenance-observers",
  "owner": "riverhog-provenance",
  "owner_constant": "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP",
  "owner_path": "packages/riverhog-provenance",
  "providers": [
    {
      "distribution": "riverhog-provenance-linux-observer",
      "name": "riverhog-linux",
      "value": "riverhog_provenance_linux_observer:OBSERVER_BINDING"
    },
    {
      "distribution": "riverhog-provenance-macos-observer",
      "name": "riverhog-macos",
      "value": "riverhog_provenance_macos_observer:OBSERVER_BINDING"
    },
    {
      "distribution": "riverhog-provenance-windows-observer",
      "name": "riverhog-windows",
      "value": "riverhog_provenance_windows_observer:OBSERVER_BINDING"
    }
  ]
}
```
