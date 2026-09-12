# schemas: BranchSetAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetadmittedevent:23dd1d777b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-751c9ba6ad5c"></a>
- <a id="s-81b45141f49b"></a>`title`: BranchSetAdmittedEvent
- <a id="s-90f8fe86215f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-99cbec3f8b61"></a>`data` | yes | #/components/schemas/BranchSetAdmittedEventData |  |
| <a id="s-0c20775d0b0d"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-db8b0a166a1c"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-2731606c328a"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-f66f2438fecc"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-5021293ee94c"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-a9160be71a4b"></a>`time` | yes | type="string" |  |
| <a id="s-c424b8e8e303"></a>`type` | yes | type="string"; const="io.riverhog.stove0.branch-set.admitted" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BranchSetAdmittedEventData](schemas-branchsetadmittedeventdata.md)

## Governing policies

- <a id="pa-c94579351936"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetAdmittedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8aa2c593f2c75b53aa50c593b8c5037816a0439827d84adb52b65acd2b7e84d4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/BranchSetAdmittedEventData"
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
      "const": "io.riverhog.stove0.branch-set.admitted",
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
  "title": "BranchSetAdmittedEvent",
  "type": "object"
}
```
