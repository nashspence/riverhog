# riverhog_client.ApiClient.begin_source_collection_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-begin-source-co-818742c383:4007164c13 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4831d0c6f6"></a>
- <a id="s-798fce85d7"></a>`distribution`: `riverhog-client`
- <a id="s-3e873f996f"></a>`module`: `riverhog_client`
- <a id="s-17cbb406f3"></a>`name`: `begin_source_collection_retirement`
- <a id="s-0023ae7285"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-c44ea48589"></a>`unit`: `member`

### Declared structure

- <a id="s-0ce9d28681"></a>`kind`: `"method"`
- <a id="s-0154773239"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-processing-claims/{claim_id}/source-collection-retirement](../../riverhog/http-operations/post-v1-collection-processing-claims-claim-id-source-collection-retirement.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-aa88ecde54"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.begin\_source\_collection\_retirement](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L672)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.begin_source_collection_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83d3af47e45dfa6f9c06472bfb2b9ca69c638be685fe76137e1ff9346352818c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "begin_source_collection_retirement",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
