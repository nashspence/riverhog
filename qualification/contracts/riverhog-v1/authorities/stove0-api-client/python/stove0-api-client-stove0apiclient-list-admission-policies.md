# stove0_api_client.Stove0ApiClient.list_admission_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-ad-795ad67f73:e9b7dc109f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef977a783f"></a>
- <a id="s-26367120a8"></a>`distribution`: `stove0-api-client`
- <a id="s-25ae68f686"></a>`module`: `stove0_api_client`
- <a id="s-099a922060"></a>`name`: `list_admission_policies`
- <a id="s-4023b786e8"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-2daa3404d8"></a>`unit`: `member`

### Declared structure

- <a id="s-1ca2d1f25b"></a>`kind`: `"method"`
- <a id="s-c6b37a4430"></a>`signature`: `"\"(self) -> 'AdmissionPolicyCatalogView'\""`

## Maintained corroboration

### Related interface records

- [stove0 admission policy list](../../stove0-client/cli/stove0-admission-policy-list.md)
- [GET /v1/admission-policies](../../stove0/http-operations/get-v1-admission-policies.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-1f75673305"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — [reference/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.list\_admission\_policies](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L159)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_admission_policies`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95cc2de6cf27b83056e0321a13a854765f35c61f722f7514105302995465242c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AdmissionPolicyCatalogView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_admission_policies",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
