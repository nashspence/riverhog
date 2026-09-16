# schemas: WorkUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workupdatedevent:4bfc4e0ba1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-46805a6e03"></a>

- <a id="s-4a84b11889"></a>`type`: `"object"`
- <a id="s-c2f1d620dc"></a>`additionalProperties`: `false`
- <a id="s-077330d907"></a>`required`: `["id","source","type","subject","time","data"]`
- <a id="s-b3978166b2"></a>`title`: `"WorkUpdatedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-089d96c784"></a>`data` | yes | [WorkUpdatedEventData](schemas-workupdatedeventdata.md) |  |
| <a id="s-0a458eb822"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-5445b0c1bb"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-dce1579c51"></a>`source` | yes | type="string"; const="urn:riverhog:stove0"; title="Source" |  |
| <a id="s-e4a4e26b10"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-7d62dec993"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-c6fb4b73c2"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-eea0cd0fd2"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.updated"; title="Type" |  |

## Maintained corroboration

### Referenced contract dossiers

- [WorkUpdatedEventData](schemas-workupdatedeventdata.md)

## Governing policies

- <a id="pa-7629c4b3cd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkUpdatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1b6b1d4b060d43dbd4be5aa2a5643cda62c6dfee7e2badf04ca9d2bbaf89a76 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/WorkUpdatedEventData"
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
      "const": "urn:riverhog:stove0",
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
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "time": {
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.work.updated",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "subject",
    "time",
    "data"
  ],
  "title": "WorkUpdatedEvent",
  "type": "object"
}
```

</details>
