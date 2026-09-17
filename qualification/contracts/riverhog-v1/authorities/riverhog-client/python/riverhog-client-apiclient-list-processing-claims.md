# riverhog_client.ApiClient.list_processing_claims

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-processing-claims:4894fa18c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a5b9be2fde"></a>
- <a id="s-5e3851cccb"></a>`distribution`: `riverhog-client`
- <a id="s-6b5edffcfc"></a>`module`: `riverhog_client`
- <a id="s-2357148fc4"></a>`name`: `list_processing_claims`
- <a id="s-4718669022"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-62540f74e7"></a>`unit`: `member`

### Declared structure

- <a id="s-742667c250"></a>`kind`: `"method"`
- <a id="s-743e01ac64"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, state: 'ClaimState \| None' = None, sort: 'ProcessingClaimSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'ProcessingClaimPageDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-processing-claims](../../riverhog/http-operations/get-v1-collection-processing-claims.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-fb29d6bcc5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.list\_processing\_claims](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L225)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_processing_claims`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4b34e30e38ad23b09249a2642844284885786ba58cf5de5eae437ad0da92519 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, state: 'ClaimState | None' = None, sort: 'ProcessingClaimSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'ProcessingClaimPageDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_processing_claims",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
