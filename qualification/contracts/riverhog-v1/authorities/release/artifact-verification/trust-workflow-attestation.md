# Trust: workflow_attestation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: artifact-verification:release:trust-workflow-attestation:9c54619fbe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Artifact Verification](index.md) |

## External contract

<a id="s-0b0e5226de"></a>
| Concern | Contract |
|---|---|
| <a id="s-2c88b08cb8"></a>`meaning` | `"Published GitHub artifacts also receive workflow identity attestations; they complement rather than replace the maintainer signature."` |
| <a id="s-ea605023cf"></a>`scheme` | `"github-oidc"` |

## Governing policies

- <a id="pa-872757acac"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/trust/workflow_attestation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ab42dee1c319687f63ba561c0128796055de2d00f11396b3cec4e9928b53989 -->

```json
{
  "meaning": "Published GitHub artifacts also receive workflow identity attestations; they complement rather than replace the maintainer signature.",
  "scheme": "github-oidc"
}
```

</details>
