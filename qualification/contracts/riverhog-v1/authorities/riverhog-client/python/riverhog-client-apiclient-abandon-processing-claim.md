# riverhog_client.ApiClient.abandon_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-abandon-processing-claim:328a7bf2ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10d91476b1"></a>
- <a id="s-69d3526309"></a>`distribution`: `riverhog-client`
- <a id="s-728ab45ba9"></a>`module`: `riverhog_client`
- <a id="s-26f869a5a1"></a>`name`: `abandon_processing_claim`
- <a id="s-1fc44cd8a0"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-e772b688b0"></a>`unit`: `member`

### Declared structure

- <a id="s-59fd5b90aa"></a>`kind`: `"method"`
- <a id="s-d356ac76ee"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', reason: 'str') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/abandon](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-abandon.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-205659feeb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/workflows.py::CollectionWorkflowMethods.abandon_processing_claim](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L273)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.abandon_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d28e3b17731059b9406c9d1629b27f1154ac19ceca7ddb291c1309df85572c12 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', reason: 'str') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "abandon_processing_claim",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
