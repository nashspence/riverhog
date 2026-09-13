# Release artifact: evidence:install-manifest.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-artifact-evidence-install-manifest-json:602849befc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-6f006232ba"></a>
| Concern | Contract |
|---|---|
| <a id="s-3cd683941c"></a>`coordinate` | install-manifest.json |
| <a id="s-ffbb51bce3"></a>`format` | json |

## Governing policies

- <a id="pa-1a47e3004d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/release_artifacts/evidence:install-manifest.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fbdbef6f09d65e5582f99e4169e1f089f0c11bf13f068a2a062dc306a9d1865 -->

```json
{
  "coordinate": "install-manifest.json",
  "format": "json"
}
```
