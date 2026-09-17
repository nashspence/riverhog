# riverhog_client.ApiClient.renew_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-renew-processing-claim:859ef5f5e9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-abec9c0fb8"></a>
- <a id="s-80ae61aa2a"></a>`distribution`: `riverhog-client`
- <a id="s-4e06ed0068"></a>`module`: `riverhog_client`
- <a id="s-3082b7dbd4"></a>`name`: `renew_processing_claim`
- <a id="s-b6473c710a"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-72b0b9fcef"></a>`unit`: `member`

### Declared structure

- <a id="s-cb1125881f"></a>`kind`: `"method"`
- <a id="s-5b34714d8b"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/renew](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-renew.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6dc5456d3a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.renew\_processing\_claim](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L253)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.renew_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa1e683a5305a89cf1d4357e2fb54207420593caa89a82e26e3e78bedc792944 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "renew_processing_claim",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
