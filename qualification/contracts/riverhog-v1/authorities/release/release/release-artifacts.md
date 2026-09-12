# Release artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-artifacts:c408be4985 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [release-contract](index.md#f-6cd3d52e18) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3a2a162a83"></a>
| Field | Shape |
|---|---|
| <a id="s-f93745455d"></a>`contract` | "riverhog-v1-contract.tar.gz" |
| <a id="s-419746c37d"></a>`documentation` | "riverhog-docs-v{version}.tar.gz" |
| <a id="s-a45436c45e"></a>`evidence` | ["riverhog-v1-contract.tar.gz","install-manifest.json","release-manifest.json","SHA256SUMS","SHA256SUMS.minisig","release.spdx.json","release.intoto.jsonl","THIRD_PARTY_NOTICES.md"] |
| <a id="s-432690295d"></a>`notices` | format="tar.gz"; additional keys=`basis`, `directory`, `required_for`, `schema` |
| <a id="s-8f4d041b89"></a>`python_formats` | ["wheel","sdist"] |
| <a id="s-27ad787f38"></a>`source` | "riverhog-source-v{version}.tar.gz" |

## Governing policies

- <a id="pa-a05cfdb296"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/artifacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcfd825e8168e132444c279da15d33ceb66eb3a7c7565af9dcb94c366e9def3f -->

```json
{
  "contract": "riverhog-v1-contract.tar.gz",
  "documentation": "riverhog-docs-v{version}.tar.gz",
  "evidence": [
    "riverhog-v1-contract.tar.gz",
    "install-manifest.json",
    "release-manifest.json",
    "SHA256SUMS",
    "SHA256SUMS.minisig",
    "release.spdx.json",
    "release.intoto.jsonl",
    "THIRD_PARTY_NOTICES.md"
  ],
  "notices": {
    "basis": "exact-artifact-contents",
    "directory": "notices",
    "format": "tar.gz",
    "required_for": [
      "wheel",
      "image"
    ],
    "schema": "riverhog-artifact-notices/v1"
  },
  "python_formats": [
    "wheel",
    "sdist"
  ],
  "source": "riverhog-source-v{version}.tar.gz"
}
```
