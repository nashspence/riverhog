# Reference component policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:reference-component-policy:661dcc27f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `repository` |
| Interface | `boundary` |
| Family | `references` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: "Checked-in references form a closed, tightly scoped, maintainer-selected, nonnormative conformance set."

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/boundaries/reference_policy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ee596dce1a6280dbfac583d7a7a3d7fabc07aa011ba421729b83179d8126cd3 -->

```json
"Checked-in references form a closed, tightly scoped, maintainer-selected, nonnormative conformance set."
```
