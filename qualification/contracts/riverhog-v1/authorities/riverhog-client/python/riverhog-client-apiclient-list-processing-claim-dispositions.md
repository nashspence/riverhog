# riverhog_client.ApiClient.list_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-processing-ff05fb0a88:a5f2e9a1fb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f62b2421d"></a>
- <a id="s-45afd7e806"></a>`distribution`: `riverhog-client`
- <a id="s-83b39ab44f"></a>`module`: `riverhog_client`
- <a id="s-c9ef1f05b6"></a>`name`: `list_processing_claim_dispositions`
- <a id="s-ca9a2aabde"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-98752b19b8"></a>`unit`: `member`

### Declared structure

- <a id="s-007a065298"></a>`kind`: `"method"`
- <a id="s-9bbbfce1c1"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, identity_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionPageDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims/{claim_id}/derivation/dispositions](../../riverhog/http-operations/get-v1-collection-processing-claims-claim-id-derivation-dispositions.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-70ff381a7d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.list\_processing\_claim\_dispositions](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L540)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_processing_claim_dispositions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c227264af45bc6eec229c7de30c4b06ccfd8bac8b5cd7097c0e8a95cad56461 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, identity_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionPageDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_processing_claim_dispositions",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
