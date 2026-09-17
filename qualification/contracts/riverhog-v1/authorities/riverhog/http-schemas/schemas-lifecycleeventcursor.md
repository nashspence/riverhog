# schemas: LifecycleEventCursor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-lifecycleeventcursor:4152eb0e29 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-83181c31b6"></a>

- <a id="s-f86e7795f3"></a>`type`: `"string"`
- <a id="s-847c90f91d"></a>`maxLength`: `19`
- <a id="s-f11e5224ba"></a>`minLength`: `1`
- <a id="s-86d40d302d"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)$"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=19; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: LifecycleEventCursor](#s-83181c31b6) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-98156fc825"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-29de0c7263"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/LifecycleEventCursor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2bd38d4ffa640e182b719183718a4ce55d6914da3bb18bfbe32e4633d2c7815 -->

```json
{
  "maxLength": 19,
  "minLength": 1,
  "pattern": "^(?:0|[1-9][0-9]*)$",
  "type": "string"
}
```

</details>
