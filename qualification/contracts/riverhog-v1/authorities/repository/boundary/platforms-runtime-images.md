# Platforms runtime images

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:platforms-runtime-images:85d134374a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `repository` |
| Interface | `boundary` |
| Family | `runtime-images` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: ["linux/amd64"]

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

- `/boundaries/runtime_images/platforms`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac8335d2e189797f6099e4d37b3fd5a69dcc42bfb5a2b077bd758e7de971ca62 -->

```json
[
  "linux/amd64"
]
```
