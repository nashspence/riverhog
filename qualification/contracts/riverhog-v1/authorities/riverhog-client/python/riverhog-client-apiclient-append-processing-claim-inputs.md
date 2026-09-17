# riverhog_client.ApiClient.append_processing_claim_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-append-processi-0e66da1fb6:512887a710 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b086589ef"></a>
- <a id="s-adf9a170f7"></a>`distribution`: `riverhog-client`
- <a id="s-9fe1894c2c"></a>`module`: `riverhog_client`
- <a id="s-9843f70cee"></a>`name`: `append_processing_claim_inputs`
- <a id="s-caee5a9382"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-a2150349f7"></a>`unit`: `member`

### Declared structure

- <a id="s-419623ce22"></a>`kind`: `"method"`
- <a id="s-88e5fe8273"></a>`signature`: `"\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', start_ordinal: 'int', inputs: 'Sequence[RootInput]') -> 'ReceivingSetDocument'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/inputs](../../riverhog/http-operations/put-v1-collection-processing-claims-claim-id-inputs.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-0f3aacb58e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/workflows.py::CollectionWorkflowMethods.append\_processing\_claim\_inputs](../../../../../../packages/riverhog-client/src/riverhog_client/workflows.py#L160)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.append_processing_claim_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f7b45181aa97157a694a07b039739938265331a0012713da3678c41488464c34 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'ProcessingClaimId', *, fence: 'int', start_ordinal: 'int', inputs: 'Sequence[RootInput]') -> 'ReceivingSetDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "append_processing_claim_inputs",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
