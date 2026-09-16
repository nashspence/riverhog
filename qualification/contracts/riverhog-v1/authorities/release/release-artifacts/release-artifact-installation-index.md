# Release artifact: installation:index

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release-artifacts:release:release-artifact-installation-index:ef2b023c66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release Artifacts](index.md) |

## External contract

<a id="s-052239b850"></a>
| Concern | Contract |
|---|---|
| <a id="s-2aa674c281"></a>`coordinate` | riverhog-python-index-v{version}.tar.gz |
| <a id="s-c3a18f4859"></a>`format` | tar+gzip |

## Governing policies

- <a id="pa-d7fb03cffa"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/release_artifacts/installation:index`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1dd82ef57408718b15e36656b4cd9e000015c4b00676b2606b3fc2d3998fd9e2 -->

```json
{
  "coordinate": "riverhog-python-index-v{version}.tar.gz",
  "format": "tar+gzip"
}
```

</details>
