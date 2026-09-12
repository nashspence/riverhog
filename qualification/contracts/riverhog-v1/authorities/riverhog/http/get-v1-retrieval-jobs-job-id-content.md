# GET /v1/retrieval-jobs/{job_id}/content

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-jobs-job-id-content:2b8e9d02d1 -->

Download Retrieval File

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c5492954bd"></a>
- <a id="s-5335e8d6ef"></a>`operationId`: download_retrieval_file
- <a id="s-b38705bf02"></a>`summary`: Download Retrieval File
- <a id="s-8748e199d3"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-4ac607f909"></a>`job_id` | path | yes | type="string" |
| <a id="s-0f44a5997d"></a>`collection_id` | query | yes | #/components/schemas/CollectionIdParameter |
| <a id="s-c2dded5d77"></a>`path` | query | yes | type="string" |
| <a id="s-b26dd4f5ac"></a>`If-Match` | header | yes | type="string"; pattern="^\"[0-9a-f]{64}\"$" |
| <a id="s-2a974ee7d8"></a>`Range` | header | no | anyOf=type="string" \| type="null" |
| <a id="s-a7a12d14ae"></a>`If-None-Match` | header | no | anyOf=type="string" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-8aae1aafce"></a>`200` | Successful Response |
| <a id="s-96028b3e13"></a>`400` | Bad Request |
| <a id="s-8c8a839936"></a>`401` | Unauthorized |
| <a id="s-34190f6e02"></a>`403` | Forbidden |
| <a id="s-ef41c9f9ca"></a>`404` | Not Found |
| <a id="s-7fc74353a8"></a>`409` | Conflict |
| <a id="s-eb7df68430"></a>`412` | Precondition Failed |
| <a id="s-fe427dd012"></a>`416` | Requested Range Not Satisfiable |
| <a id="s-d380cee0b5"></a>`429` | Too Many Requests |
| <a id="s-fd7dcc0ebc"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: download_retrieval_file](../operation/operation-parity-download-retrieval-file.md)

### Referenced contract dossiers

- [schemas: CollectionIdParameter](schemas-collectionidparameter.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-7db824b548"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs~1{job_id}~1content/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cafaf341ac9a8ea2eee63c5c1503cbc0b1ae38ce86d6e8ceb7075d031ce1950 -->

```json
{
  "operationId": "download_retrieval_file",
  "parameters": [
    {
      "in": "path",
      "name": "job_id",
      "required": true,
      "schema": {
        "title": "Job Id",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "collection_id",
      "required": true,
      "schema": {
        "$ref": "#/components/schemas/CollectionIdParameter"
      }
    },
    {
      "in": "query",
      "name": "path",
      "required": true,
      "schema": {
        "title": "Path",
        "type": "string"
      }
    },
    {
      "in": "header",
      "name": "If-Match",
      "required": true,
      "schema": {
        "pattern": "^\"[0-9a-f]{64}\"$",
        "title": "If-Match",
        "type": "string"
      }
    },
    {
      "in": "header",
      "name": "Range",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Range"
      }
    },
    {
      "in": "header",
      "name": "If-None-Match",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "If-None-Match"
      }
    }
  ],
  "responses": {
    "200": {
      "description": "Successful Response"
    },
    "400": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Bad Request",
      "x-riverhog-error-codes": [
        "bad_request"
      ]
    },
    "401": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Unauthorized",
      "x-riverhog-error-codes": [
        "unauthorized"
      ]
    },
    "403": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Forbidden",
      "x-riverhog-error-codes": [
        "forbidden"
      ]
    },
    "404": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Not Found",
      "x-riverhog-error-codes": [
        "not_found"
      ]
    },
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Conflict",
      "x-riverhog-error-codes": [
        "invalid_state"
      ]
    },
    "412": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Precondition Failed",
      "x-riverhog-error-codes": [
        "precondition_failed"
      ]
    },
    "416": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Requested Range Not Satisfiable",
      "x-riverhog-error-codes": [
        "invalid_range"
      ]
    },
    "429": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Too Many Requests",
      "x-riverhog-error-codes": [
        "download_allowance_exceeded"
      ]
    },
    "500": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Internal Server Error",
      "x-riverhog-error-codes": [
        "internal_error"
      ]
    }
  },
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Download Retrieval File",
  "tags": [
    "retrieval"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "retrieval:manage"
      ]
    }
  ]
}
```
