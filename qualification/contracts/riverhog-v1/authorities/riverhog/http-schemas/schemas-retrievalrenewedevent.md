# schemas: RetrievalRenewedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalrenewedevent:202de8058a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2828b0debe"></a>

- <a id="s-6219101b14"></a>`type`: `"object"`
- <a id="s-18f77acfd0"></a>`additionalProperties`: `false`
- <a id="s-ee2e806a02"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-13bd63e05e"></a>`title`: `"RetrievalRenewedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2fd48f2e9"></a>`data` | yes | #/components/schemas/RetrievalRenewedData |  |
| <a id="s-0177fe3a1e"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-debf7f64f3"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-1a2491dcf0"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-a5eda468f4"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-55d9d8850d"></a>`subject` | no | anyOf=(type="string"; minLength=1) \| (type="null") |  |
| <a id="s-38d57bfa46"></a>`time` | yes | type="string" |  |
| <a id="s-46bb4a9bf2"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.renewed" |  |

## Maintained corroboration

### Referenced contract dossiers

- [RetrievalRenewedData](schemas-retrievalreneweddata.md)

## Governing policies

- <a id="pa-dd6ddf8832"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalRenewedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9001f95e3ea4df43a5fbffa94c37a265cd666d62d38980ecfa4582c327fe3896 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalRenewedData"
    },
    "datacontenttype": {
      "const": "application/json",
      "default": "application/json",
      "title": "Datacontenttype",
      "type": "string"
    },
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "minLength": 1,
      "title": "Source",
      "type": "string"
    },
    "specversion": {
      "const": "1.0",
      "default": "1.0",
      "title": "Specversion",
      "type": "string"
    },
    "subject": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "time": {
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.riverhog.retrieval.renewed",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "time",
    "data"
  ],
  "title": "RetrievalRenewedEvent",
  "type": "object"
}
```

</details>
