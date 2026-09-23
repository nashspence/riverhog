# generated:review0-sampler protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol:review0-sampler-lib:generated-review0-sampler-protocol:a72ec89843 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol](index.md) |

## External contract

<a id="s-2e38f22035"></a>
<a id="s-d06809329d"></a>

| Field | Value |
|---|---|
| <a id="s-cc3673cdeb"></a>`authorities · http_operations` | `"http_binding.operations"` |
| <a id="s-1d737f3aa2"></a>`authorities · semantic_acceptance` | `"semantic_acceptance"` |
| <a id="s-43d11b7c9d"></a>`authorities · structural_models` | `"schemas"` |
| <a id="s-cd25dd0347"></a>`bundle_sha256` | `"cbb092158be9d9cc515fffa79a10d2de24145a0d759667a7af6abc773e24a711"` |
| <a id="s-c08d2f9d50"></a>`format` | `"review0-sampler-schema-bundle/v1"` |
| <a id="s-772400cd68"></a>`protocol` | `"review0-sampler/v1"` |
| <a id="s-8bdb330a36"></a>`semantic_acceptance · kind` | `"request-bound-result"` |
| <a id="s-b00b1d8891"></a>`semantic_acceptance · validator` | `"validate_result"` |

## Maintained corroboration

### Related interface records

- [GET /v1/sampler](../process-protocol-operations/get-v1-sampler.md)
- [POST /v1/sample](../process-protocol-operations/post-v1-sample.md)
- [generated:review0-sampler: ErrorResponse](../process-protocol-schemas/generated-review0-sampler-errorresponse.md)
- [generated:review0-sampler: SamplerConformanceResult](../process-protocol-schemas/generated-review0-sampler-samplerconformanceresult.md)
- [generated:review0-sampler: SamplerDescriptor](../process-protocol-schemas/generated-review0-sampler-samplerdescriptor.md)
- [generated:review0-sampler: SamplerRequest](../process-protocol-schemas/generated-review0-sampler-samplerrequest.md)
- [generated:review0-sampler: SamplerResult](../process-protocol-schemas/generated-review0-sampler-samplerresult.md)

## Governing policies

- <a id="pa-d86e074ca4"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/authorities`
- `/external_contract/protocol_schemas/generated:review0-sampler/bundle_sha256`
- `/external_contract/protocol_schemas/generated:review0-sampler/format`
- `/external_contract/protocol_schemas/generated:review0-sampler/protocol`
- `/external_contract/protocol_schemas/generated:review0-sampler/semantic_acceptance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:review0-sampler/authorities`

<!-- exact-contract-value: 723bdc629c692bd71cc54f046e71fc791297a8c6a984b85d1f0962ee4c72194e -->

```json
{
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:review0-sampler/bundle_sha256`

<!-- exact-contract-value: aaa7eeab2831961f25cd5a11aa7e519e0d7bc5ac22bf05a773b7425c97b91416 -->

```json
"cbb092158be9d9cc515fffa79a10d2de24145a0d759667a7af6abc773e24a711"
```

### `/external_contract/protocol_schemas/generated:review0-sampler/format`

<!-- exact-contract-value: b1ba450c6a92e7eb0506d9e9a11f9d9981e6a55e186e86859e7597d217b79ea0 -->

```json
"review0-sampler-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:review0-sampler/protocol`

<!-- exact-contract-value: b6eb48b5527558224f96b731e3536a966831b259cc2840e3c8c6d3491785ab54 -->

```json
"review0-sampler/v1"
```

### `/external_contract/protocol_schemas/generated:review0-sampler/semantic_acceptance`

<!-- exact-contract-value: 6f3fe98e15830a6dbf8fe7673f37379515ecba550b1a0fde654082162bfb0ad3 -->

```json
{
  "kind": "request-bound-result",
  "validator": "validate_result"
}
```

</details>
