# schemas: JoinAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joinadmittedevent:e1d574a1f4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-89c2b9e5301e"></a>
- <a id="s-b5df77a5beb6"></a>`title`: JoinAdmittedEvent
- <a id="s-797a81b1255f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7dff2842c26b"></a>`data` | yes | #/components/schemas/JoinAdmittedEventData |  |
| <a id="s-2ce0d771042a"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-3adf064b80f4"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-0800c849d677"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-443d205ec8cf"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-ae8b6da30340"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-6d92c967116f"></a>`time` | yes | type="string" |  |
| <a id="s-dbf4280eb9b6"></a>`type` | yes | type="string"; const="io.riverhog.stove0.join.admitted" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JoinAdmittedEventData](schemas-joinadmittedeventdata.md)

## Governing policies

- <a id="pa-64765916f07b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinAdmittedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 399c6d1daf10ceea8b1ddf3dc271deb0f1c71c74d764005f54cb24ca5afde635 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/JoinAdmittedEventData"
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
      "const": "io.riverhog.stove0.join.admitted",
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
  "title": "JoinAdmittedEvent",
  "type": "object"
}
```
