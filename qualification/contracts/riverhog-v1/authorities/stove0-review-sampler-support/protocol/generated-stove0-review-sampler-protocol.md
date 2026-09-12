# generated:stove0-review-sampler protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-protocol:8f6ec4c996 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [protocol](index.md#f-1e0cecac00) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Subject | Shape |
|---|---|
| <a id="s-1c9b12a3fd"></a>`authorities` | additional keys=`http_operations`, `semantic_acceptance`, `structural_models` |
| <a id="s-0bdd9b0af3"></a>`bundle_sha256` | "dbbc9320e223fb591a30981339ba8c7a7f8b7de2358ce3a3cc38c2c7fbf78119" |
| <a id="s-70d47dd5f8"></a>`format` | "stove0-review-sampler-schema-bundle/v1" |
| <a id="s-54f1345bc6"></a>`http_binding` | additional keys=`operations` |
| <a id="s-835f094545"></a>`protocol` | "stove0-review-sampler/v1" |
| <a id="s-aacaa6d4b2"></a>`semantic_acceptance` | additional keys=`kind`, `validator` |

## Governing policies

- <a id="pa-1084bd8da6"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/authorities`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/format`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/http_binding`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/protocol`
- `/external_contract/protocol_schemas/generated:stove0-review-sampler/semantic_acceptance`

### Exact owned JSON

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

### `/external_contract/protocol_schemas/generated:stove0-review-sampler/http_binding`

<!-- exact-contract-value: 932a5226f67bc5ad2fb5722c63cafd6b0c92dc34d172f256110f50a9af083d9d -->

```json
{
  "operations": [
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "bad_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "sampler_failed",
          "status": 500
        }
      ],
      "method": "GET",
      "path": "/v1/sampler",
      "path_parameters": [],
      "request": {
        "kind": "none",
        "schema": null
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "SamplerDescriptor",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "invalid_sampler_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "sampler_changed",
          "status": 409
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "sampler_failed",
          "status": 500
        }
      ],
      "method": "POST",
      "path": "/v1/sample",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "SamplerRequest"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "SamplerResult",
        "statuses": [
          200
        ]
      }
    }
  ]
}
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
