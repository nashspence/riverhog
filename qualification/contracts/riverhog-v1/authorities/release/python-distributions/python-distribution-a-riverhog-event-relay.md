# Python distribution: a-riverhog-event-relay

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-riverhog-event-relay:5a811c6012 -->

Native lifecycle-event to CloudEvents webhook relay.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-5228778e2c"></a>
| Concern | Contract |
|---|---|
| <a id="s-d8355ea46a"></a>`artifacts` | `[{"coordinate":"dist/a_riverhog_event_relay-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_riverhog_event_relay-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-a245141b83"></a>`channel` | `"github-release"` |
| <a id="s-685f5b5a03"></a>`description` | `"Native lifecycle-event to CloudEvents webhook relay."` |
| <a id="s-4b3b00d4a1"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-bc591abc1f"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-6e0207dace"></a>`publication_identity` | `{"coordinate":"a-riverhog-event-relay","kind":"python-distribution"}` |
| <a id="s-698b5a2bac"></a>`requires_python` | `">=3.12"` |
| <a id="s-1e293e3907"></a>`role` | `"application"` |
| <a id="s-75e57e8d46"></a>`source` | `"some-implementations/riverhog/applications/a-riverhog-event-relay/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-event-relay](../../../evidence/relationships/nodes.md#rn-ad16b219fd)

## Governing policies

- <a id="pa-d110399a52"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-795d59c609"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-event-relay](../../../evidence/sources/authorities.md#src-9733307981) — [some-implementations/riverhog/applications/a-riverhog-event-relay/pyproject.toml](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-riverhog-event-relay`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd591853e83b578615b85255c250468a32800cf70ed0021b69b200d418d66467 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_riverhog_event_relay-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_riverhog_event_relay-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Native lifecycle-event to CloudEvents webhook relay.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-riverhog-event-relay",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "application",
  "source": "some-implementations/riverhog/applications/a-riverhog-event-relay/pyproject.toml"
}
```

</details>
