# Compatibility: durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-durable-state:e2e7d5faf0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract


| Field | Value |
|---|---|
| <a id="s-ac3d88d871"></a>`durable_state` | `"Every later v1 release applies each durable-state authority's declared transition rule to state created by every earlier v1 release; this does not promise downgrade support or earlier software reading later state."` |

## Governing policies

- <a id="pa-e5aa68bd57"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/compatibility/durable_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 903340d7ff46140702f3c53400b8444ca984646ea458ebda76acc9b5025a4c20 -->

```json
"Every later v1 release applies each durable-state authority's declared transition rule to state created by every earlier v1 release; this does not promise downgrade support or earlier software reading later state."
```

</details>
