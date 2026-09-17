# generated:stove0-review-sampler protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol:stove0-review-sampler-support:generated-stove0-review-sampler-protocol:e4a02b1b92 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol](index.md) |

## External contract

<a id="s-aacaa6d4b2"></a>
<a id="s-1c9b12a3fd"></a>

| Field | Value |
|---|---|
| <a id="s-a47a617b1d"></a>`authorities · http_operations` | `"http_binding.operations"` |
| <a id="s-86ee5506af"></a>`authorities · semantic_acceptance` | `"semantic_acceptance"` |
| <a id="s-ac2d9ba033"></a>`authorities · structural_models` | `"schemas"` |
| <a id="s-0bdd9b0af3"></a>`bundle_sha256` | `"dbbc9320e223fb591a30981339ba8c7a7f8b7de2358ce3a3cc38c2c7fbf78119"` |
| <a id="s-70d47dd5f8"></a>`format` | `"stove0-review-sampler-schema-bundle/v1"` |
| <a id="s-835f094545"></a>`protocol` | `"stove0-review-sampler/v1"` |
| <a id="s-ccbb501261"></a>`semantic_acceptance · kind` | `"request-bound-result"` |
| <a id="s-554cd42af9"></a>`semantic_acceptance · validator` | `"validate_result"` |

## Maintained corroboration

### Related interface records

- [GET /v1/sampler](../process-protocol-operations/get-v1-sampler.md)
- [POST /v1/sample](../process-protocol-operations/post-v1-sample.md)
- [generated:stove0-review-sampler: ErrorResponse](../process-protocol-schemas/generated-stove0-review-sampler-errorresponse.md)
- [generated:stove0-review-sampler: SamplerConformanceResult](../process-protocol-schemas/generated-stove0-review-sampler-samplerconformanceresult.md)
- [generated:stove0-review-sampler: SamplerDescriptor](../process-protocol-schemas/generated-stove0-review-sampler-samplerdescriptor.md)
- [generated:stove0-review-sampler: SamplerRequest](../process-protocol-schemas/generated-stove0-review-sampler-samplerrequest.md)
- [generated:stove0-review-sampler: SamplerResult](../process-protocol-schemas/generated-stove0-review-sampler-samplerresult.md)

## Governing policies

- <a id="pa-26c1ec8ccb"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/schemas.py::sampler\_schema\_bundle](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/authorities`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/format`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/protocol`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/semantic_acceptance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:stove0-review-sampler/authorities`

<!-- exact-contract-value: 723bdc629c692bd71cc54f046e71fc791297a8c6a984b85d1f0962ee4c72194e -->

```json
{
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:stove0-review-sampler/bundle_sha256`

<!-- exact-contract-value: 552a20034f23023138a0710253c4c2787d7a61d31d0e886e1b8a6e4429785cac -->

```json
"dbbc9320e223fb591a30981339ba8c7a7f8b7de2358ce3a3cc38c2c7fbf78119"
```

### `/external_contract/protocol_schemas/generated:stove0-review-sampler/format`

<!-- exact-contract-value: 4f3bad5b7b673bc466cdef41d99d0645dc1211bb41133d7b08c1284a38e46de2 -->

```json
"stove0-review-sampler-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:stove0-review-sampler/protocol`

<!-- exact-contract-value: 353700e86862a7c8a411ee1fb49615e9e6f37401a1def5b49e69bc95b8cd088d -->

```json
"stove0-review-sampler/v1"
```

### `/external_contract/protocol_schemas/generated:stove0-review-sampler/semantic_acceptance`

<!-- exact-contract-value: 6f3fe98e15830a6dbf8fe7673f37379515ecba550b1a0fde654082162bfb0ad3 -->

```json
{
  "kind": "request-bound-result",
  "validator": "validate_result"
}
```

</details>
