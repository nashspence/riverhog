# Release artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-artifacts:c408be4985 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `release-contract` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `contract` | "riverhog-v1-contract.tar.gz" |
| `documentation` | "riverhog-docs-v{version}.tar.gz" |
| `evidence` | ["riverhog-v1-contract.tar.gz","install-manifest.json","release-manifest.json","SHA256SUMS","SHA256SUMS.minisig","release.spdx.json","release.intoto.jsonl","THIRD_PARTY_NOTICES.md"] |
| `notices` | format="tar.gz"; additional keys=`basis`, `directory`, `required_for`, `schema` |
| `python_formats` | ["wheel","sdist"] |
| `source` | "riverhog-source-v{version}.tar.gz" |

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

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
