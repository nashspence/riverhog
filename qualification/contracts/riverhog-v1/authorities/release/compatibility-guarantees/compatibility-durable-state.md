# Compatibility: durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-durable-state:e2e7d5faf0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="p-214a49c2de"></a>
[Indexed applications](../../../policies/compatibility-durable-state-v1/applications.md)


| Field | Value |
|---|---|
| <a id="s-ac3d88d871"></a>`durable_state` | `"Every later v1 release applies each durable-state authority's declared transition rule to state created by every earlier v1 release; this does not promise downgrade support or earlier software reading later state."` |

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

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
