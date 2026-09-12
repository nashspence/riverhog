# Release artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-artifacts:c408be4985 -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `release-contract` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/artifacts`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

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
