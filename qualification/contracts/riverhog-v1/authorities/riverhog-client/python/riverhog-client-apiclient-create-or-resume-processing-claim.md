# riverhog_client.ApiClient.create_or_resume_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-or-resum-f0a78d29f1:d9b77d0321 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3a2a8793af"></a>
- <a id="s-d7275c0733"></a>`distribution`: `riverhog-client`
- <a id="s-874a5286d0"></a>`module`: `riverhog_client`
- <a id="s-e8f45e9bee"></a>`name`: `create_or_resume_processing_claim`
- <a id="s-4c7d640468"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-a30f8aea37"></a>`unit`: `member`

### Declared structure

- <a id="s-b9afb4a9dd"></a>`kind`: `"method"`
- <a id="s-01b98ef051"></a>`signature`: `"\"(self, *, work_id: 'str', work_document: 'Mapping[str, Any]', work_document_sha256: 'str', inputs: 'Iterable[RootInput]', lease_seconds: 'int' = 1800, purpose: 'str' = 'collection-work/v1') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims](../../riverhog/http-operations/post-v1-collection-processing-claims.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-80db313354"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.create\_or\_resume\_processing\_claim](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L121)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_or_resume_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 620f927ac56a114346133029758982dab99d1010be7444d26f8914319c9141e3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, work_id: 'str', work_document: 'Mapping[str, Any]', work_document_sha256: 'str', inputs: 'Iterable[RootInput]', lease_seconds: 'int' = 1800, purpose: 'str' = 'collection-work/v1') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_or_resume_processing_claim",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
