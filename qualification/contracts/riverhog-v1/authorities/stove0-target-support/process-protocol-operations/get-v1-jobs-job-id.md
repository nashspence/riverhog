# GET /v1/jobs/{job_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-target-support:get-v1-jobs-job-id:f1533c5339 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-bc8de15cff"></a>
| Concern | Contract |
|---|---|
| <a id="s-395852106c"></a>`error_schema` | `"ErrorResponse"` |
| <a id="s-4d97d84a6b"></a>`errors` | `[{"code":"bad_request","status":400},{"code":"unauthorized","status":401},{"code":"job_not_found","status":404},{"code":"target_failed","status":500}]` |
| <a id="s-aaadd92710"></a>`method` | `"GET"` |
| <a id="s-73ee050335"></a>`path` | `"/v1/jobs/{job_id}"` |
| <a id="s-dc07d67b9f"></a>`path_parameters` | `[{"name":"job_id","schema":{"pattern":"^[0-9a-f]{64}$","type":"string"}}]` |
| <a id="s-eefa80c700"></a>`request` | `{"kind":"none","schema":null}` |
| <a id="s-5099fa7ca8"></a>`response` | `{"headers":[],"kind":"json","schema":"TargetJobStatus","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

- <a id="pa-ce36c98d69"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [reference/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/http_binding/operations/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df0392abf514c5bb8b5b56663fb3e0a5c308ecd37b4a74cad4d92437a43de791 -->

```json
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
      "code": "job_not_found",
      "status": 404
    },
    {
      "code": "target_failed",
      "status": 500
    }
  ],
  "method": "GET",
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
    "kind": "none",
    "schema": null
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
