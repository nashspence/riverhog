# generated:stove0-target protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol:stove0-target-support:generated-stove0-target-protocol:57554dfc23 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol](index.md) |

## External contract

<a id="s-19ff2c9676"></a>
<a id="s-9269284132"></a>
<a id="s-459baccad4"></a>

| Field | Value |
|---|---|
| <a id="s-5cae7e4762"></a>`authorities · http_operations` | `"http_binding.operations"` |
| <a id="s-33a0a01689"></a>`authorities · semantic_acceptance` | `"semantic_acceptance"` |
| <a id="s-6508281ade"></a>`authorities · structural_models` | `"schemas"` |
| <a id="s-1181fd9fea"></a>`bundle_sha256` | `"916d26a1630afc12fe746ce28f8ba0f2639264e14929dd60b02c6c9059444b80"` |
| <a id="s-a8b1495338"></a>`compatibility · contract_identity` | `"rfc8785-sha256"` |
| <a id="s-61caf561fe"></a>`compatibility · unknown_fields` | `"reject"` |
| <a id="s-cb84c5173d"></a>`compatibility · unknown_protocol_revision` | `"reject"` |
| <a id="s-e51e46fd84"></a>`format` | `"stove0-target-schema-bundle/v1"` |
| <a id="s-aeb93f5dc6"></a>`protocols` | `["stove0-transform-target/v1","stove0-effect-target/v1"]` |
| <a id="s-f06050ba6c"></a>`semantic_acceptance · binding` | `"OperationContract.intent_semantics"` |
| <a id="s-476e256aa7"></a>`semantic_acceptance · identity` | `["id","profile_sha256"]` |
| <a id="s-868e8c2ec7"></a>`semantic_acceptance · kind` | `"operation-contract"` |
| <a id="s-0543785f8d"></a>`semantic_acceptance · request_response_relations` | `"required"` |

## Maintained corroboration

### Related interface records

- [GET /v1/jobs/{job_id}](../process-protocol-operations/get-v1-jobs-job-id.md)
- [GET /v1/target](../process-protocol-operations/get-v1-target.md)
- [POST /v1/jobs/{job_id}/cancel](../process-protocol-operations/post-v1-jobs-job-id-cancel.md)
- [POST /v1/preflight](../process-protocol-operations/post-v1-preflight.md)
- [PUT /v1/jobs/{job_id}](../process-protocol-operations/put-v1-jobs-job-id.md)
- [generated:stove0-target: ErrorResponse](../process-protocol-schemas/generated-stove0-target-errorresponse.md)
- [generated:stove0-target: OperationContract](../process-protocol-schemas/generated-stove0-target-operationcontract.md)
- [generated:stove0-target: TargetConformanceResult](../process-protocol-schemas/generated-stove0-target-targetconformanceresult.md)
- [generated:stove0-target: TargetContract](../process-protocol-schemas/generated-stove0-target-targetcontract.md)
- [generated:stove0-target: TargetJobRequest](../process-protocol-schemas/generated-stove0-target-targetjobrequest.md)
- [generated:stove0-target: TargetJobStatus](../process-protocol-schemas/generated-stove0-target-targetjobstatus.md)
- [generated:stove0-target: TargetPreflightRequest](../process-protocol-schemas/generated-stove0-target-targetpreflightrequest.md)
- [generated:stove0-target: TargetPreflightResponse](../process-protocol-schemas/generated-stove0-target-targetpreflightresponse.md)

## Governing policies

- <a id="pa-192c712a44"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-target](../../../evidence/sources.md#src-2c42f9d39a) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/authorities`
- `/external_contract/protocol_schemas/generated:stove0-target/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-target/compatibility`
- `/external_contract/protocol_schemas/generated:stove0-target/format`
- `/external_contract/protocol_schemas/generated:stove0-target/protocols`
- `/external_contract/protocol_schemas/generated:stove0-target/semantic_acceptance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:stove0-target/authorities`

<!-- exact-contract-value: 723bdc629c692bd71cc54f046e71fc791297a8c6a984b85d1f0962ee4c72194e -->

```json
{
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/bundle_sha256`

<!-- exact-contract-value: ffd3e8eb99f8691bc44129539a9b36eab6e0c3d559d54326a84916f6e55e374d -->

```json
"916d26a1630afc12fe746ce28f8ba0f2639264e14929dd60b02c6c9059444b80"
```

### `/external_contract/protocol_schemas/generated:stove0-target/compatibility`

<!-- exact-contract-value: fb2ae5072cf53b5181c923503273657248116b12ab23daed566f2fd061e53e27 -->

```json
{
  "contract_identity": "rfc8785-sha256",
  "unknown_fields": "reject",
  "unknown_protocol_revision": "reject"
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/format`

<!-- exact-contract-value: 6844917b9246b56594c9d97f148456b7e55cc6a0d13cb3cad09d1d8af02666c7 -->

```json
"stove0-target-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:stove0-target/protocols`

<!-- exact-contract-value: 6fa34737ad43c7376f68219f2deb607e97cb65bd84cd3d10c9372dd01ddb8d77 -->

```json
[
  "stove0-transform-target/v1",
  "stove0-effect-target/v1"
]
```

### `/external_contract/protocol_schemas/generated:stove0-target/semantic_acceptance`

<!-- exact-contract-value: 69c1750b5af0c70f85cd8837f26d3095cd1eff97666f606198ef17766c2167a8 -->

```json
{
  "binding": "OperationContract.intent_semantics",
  "identity": [
    "id",
    "profile_sha256"
  ],
  "kind": "operation-contract",
  "request_response_relations": "required"
}
```

</details>
