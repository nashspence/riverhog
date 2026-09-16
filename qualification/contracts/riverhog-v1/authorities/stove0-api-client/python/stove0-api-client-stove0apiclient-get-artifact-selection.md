# stove0_api_client.Stove0ApiClient.get_artifact_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-get-art-1e6ab70e8f:7c97006575 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c3ff0e24c"></a>
- <a id="s-c2190e207a"></a>`distribution`: `stove0-api-client`
- <a id="s-e37ef72a87"></a>`module`: `stove0_api_client`
- <a id="s-828cc629fe"></a>`name`: `get_artifact_selection`
- <a id="s-af81dad56c"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-cf6369b3f1"></a>`unit`: `member`

### Declared structure

- <a id="s-be56137fee"></a>`kind`: `"method"`
- <a id="s-0c9d5e143d"></a>`signature`: `"\"(self, selection_sha256: 'str', *, continuation: 'str \| None' = None) -> 'ArtifactSelectionPage'\""`

## Maintained corroboration

### Related interface records

- [stove0 selection show](../../stove0-client/cli/stove0-selection-show.md)
- [GET /v1/artifact-selections/{selection_sha256}](../../stove0/http-operations/get-v1-artifact-selections-selection-sha256.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-78bb3cd3dd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.get_artifact_selection](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L294)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.get_artifact_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4f702952cd7a23d150db522e9594b5bc251dc51a8b69aec67b286745db74fba -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', *, continuation: 'str | None' = None) -> 'ArtifactSelectionPage'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "get_artifact_selection",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
