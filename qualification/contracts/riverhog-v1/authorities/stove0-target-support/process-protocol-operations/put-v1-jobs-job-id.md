# PUT /v1/jobs/{job_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-target-support:put-v1-jobs-job-id:9d9877a682 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-f02f594197"></a>
| Concern | Contract |
|---|---|
| <a id="s-ae4a1c5ca2"></a>`error_schema` | `"ErrorResponse"` |
| <a id="s-0d3ef617f1"></a>`errors` | `[{"code":"invalid_target_request","status":400},{"code":"unauthorized","status":401},{"code":"request_too_large","status":413},{"code":"job_identity_mismatch","status":409},{"code":"target_contract_mismatch","status":409},{"code":"operation_contract_mismatch","status":409},{"code":"job_request_mismatch","status":409},{"code":"target_runtime_mismatch","status":409},{"code":"unsupported_operation","status":400},{"code":"target_failed","status":500}]` |
| <a id="s-10df3c4a05"></a>`method` | `"PUT"` |
| <a id="s-81cff57a99"></a>`path` | `"/v1/jobs/{job_id}"` |
| <a id="s-0e02dc2517"></a>`path_parameters` | `[{"name":"job_id","schema":{"pattern":"^[0-9a-f]{64}$","type":"string"}}]` |
| <a id="s-90500a1085"></a>`request` | `{"kind":"json","schema":"TargetJobRequest"}` |
| <a id="s-567d883078"></a>`response` | `{"headers":[],"kind":"json","schema":"TargetJobStatus","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

- <a id="pa-02ea72597b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources.md#src-2c42f9d39a) — [reference/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/http_binding/operations/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dbde0671cd2e59594910141b40ed28f6774ad83078bc480805fce76f62c09014 -->

```json
{
  "error_schema": "ErrorResponse",
  "errors": [
    {
      "code": "invalid_target_request",
      "status": 400
    },
    {
      "code": "unauthorized",
      "status": 401
    },
    {
      "code": "request_too_large",
      "status": 413
    },
    {
      "code": "job_identity_mismatch",
      "status": 409
    },
    {
      "code": "target_contract_mismatch",
      "status": 409
    },
    {
      "code": "operation_contract_mismatch",
      "status": 409
    },
    {
      "code": "job_request_mismatch",
      "status": 409
    },
    {
      "code": "target_runtime_mismatch",
      "status": 409
    },
    {
      "code": "unsupported_operation",
      "status": 400
    },
    {
      "code": "target_failed",
      "status": 500
    }
  ],
  "method": "PUT",
  "path": "/v1/jobs/{job_id}",
  "path_parameters": [
    {
      "name": "job_id",
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      }
    }
  ],
  "request": {
    "kind": "json",
    "schema": "TargetJobRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "TargetJobStatus",
    "statuses": [
      200
    ]
  }
}
```

</details>
