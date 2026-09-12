# http_api_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts:282e40cb09 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-f18ce1367ca4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-06ea1743bfa2"></a>
| Field | Shape |
|---|---|
| <a id="s-eb2aec503d16"></a>`distribution` | "http-api-contracts" |
| <a id="s-e5bb74c62cbf"></a>`exports` | additional keys=`BrowsePageToken`, `BrowseQuery`, `BrowseScalar`, `BrowseTokenCodec`, `BrowseTokenError`, `CANONICAL_VISIBLE_TEXT_PATTERN`, `CanonicalVisibleText`, `DEFAULT_HTTP_ERROR_AUTHORITY`, `ERROR_STATUS_BY_CODE`, `ErrorBody`, `ErrorResponse`, `FRAMED_BODY_DECLARATION_LENGTH_BYTES`, `FRAMED_BODY_FORMAT`, `FRAMED_BODY_MAXIMUM_DECLARATION_BYTES`, `FRAMED_BODY_MEDIA_TYPE`, `HealthResponse`, `HttpBodyKind`, `HttpErrorContract`, `HttpOperationContract`, `HttpOperationErrorAuthority`, `HttpPathParameterContract`, `HttpResponseHeaderContract`, `JSON_SEQUENCE_MEDIA_TYPE`, `MAX_BROWSE_QUERY_CHARACTERS`, `MAX_BROWSE_TOKEN_BYTES`, `OperationInterface`, `PUBLIC_ERROR_CODES`, `QuotedSha256Identity`, `Sha256Identity`, `apply_openapi_error_contract`, `canonical_json_bytes`, `cursor_feed_operation`, `error_code_for_status`, `error_payload`, `error_responses`, `exact_authority_page_operation`, `exact_set_page_operation`, `http_operation_for_request`, `http_operation_inventory`, `inline_type_schema`, `iter_json_sequence_records`, `mutable_browse_operation`, `operation_interface`, `operation_openapi`, `parse_declared_error_payload`, `parse_error_payload`, `parse_operation_error_payload`, `parse_quoted_sha256_identity`, `quote_sha256_identity`, `safe_http_base_url`, `status_for_error_code`, `structural_model_catalog`, `validate_browse_query`, `validate_sha256_identity` |
| <a id="s-dfedde9b77bd"></a>`module` | "http_api_contracts" |

## Governing policies

- <a id="pa-2e3c6791b197"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts](../../../evidence/sources.md#src-721cfe4d9d0d) — `packages/http-api-contracts/src/http_api_contracts/__init__.py::<module>`

### Machine authority

- `/external_contract/python/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c05018e519c7dd416cb1a5e1af5d93a19aead3881651c16fe485d9f3cb04759 -->

```json
{
  "distribution": "http-api-contracts",
  "exports": {
    "BrowsePageToken": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=8192, pattern=None, ascii_only=None)]"
    },
    "BrowseQuery": {
      "kind": "type-alias",
      "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), AfterValidator(func=<function validate_browse_query>)]"
    },
    "BrowseScalar": {
      "kind": "type-alias",
      "value": "str | int | bool | bytes | None"
    },
    "BrowseTokenCodec": {
      "kind": "class",
      "members": {
        "issue": {
          "kind": "method",
          "signature": "(self, *, operation: 'str', principal: 'object', selectors: 'Mapping[str, object]', position: 'Sequence[BrowseScalar]') -> 'str'"
        },
        "verify": {
          "kind": "method",
          "signature": "(self, token: 'str | None', *, operation: 'str', principal: 'object', selectors: 'Mapping[str, object]') -> 'tuple[BrowseScalar, ...] | None'"
        }
      },
      "signature": "(signing_key: 'str | bytes', *, lifetime_seconds: 'int', clock: 'Callable[[], float]' = <built-in function time>) -> 'None'"
    },
    "BrowseTokenError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "CANONICAL_VISIBLE_TEXT_PATTERN": {
      "kind": "constant",
      "value": "^\\S(?:[\\s\\S]*\\S)?$"
    },
    "CanonicalVisibleText": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "DEFAULT_HTTP_ERROR_AUTHORITY": {
      "kind": "object",
      "type": "http_api_contracts.HttpOperationErrorAuthority"
    },
    "ERROR_STATUS_BY_CODE": {
      "kind": "constant",
      "value": {
        "bad_request": 400,
        "catalog_sync_cursor_expired": 410,
        "catalog_sync_history_expired": 410,
        "catalog_sync_source_changed": 409,
        "catalog_sync_view_changed": 409,
        "conflict": 409,
        "download_allowance_exceeded": 429,
        "forbidden": 403,
        "hash_mismatch": 409,
        "ingress_failed": 500,
        "input_upload_storage_hint_invalid": 409,
        "insufficient_storage": 507,
        "internal_error": 500,
        "invalid_path": 400,
        "invalid_range": 416,
        "invalid_state": 409,
        "invalid_target": 400,
        "job_template_revision_conflict": 409,
        "length_required": 411,
        "method_not_allowed": 405,
        "not_found": 404,
        "precondition_failed": 412,
        "precondition_required": 428,
        "service_unavailable": 503,
        "storage_hint_mismatch": 409,
        "submission_conflict": 409,
        "too_many_active_input_uploads": 429,
        "unauthorized": 401
      }
    },
    "ErrorBody": {
      "kind": "class",
      "schema_sha256": "6760c34bd592b6639dc349282b6e02f6cebfa1985aa1a5b2c1943c0faa367741",
      "signature": "(*, code: Annotated[str, MinLen(min_length=1)], message: Annotated[str, MinLen(min_length=1)], details: dict[str, typing.Any] | None = None) -> None"
    },
    "ErrorResponse": {
      "kind": "class",
      "schema_sha256": "d60b687f27e4869742bff79f639b284dc47aeb1f716d9e75178e79b171ac4f9f",
      "signature": "(*, error: http_api_contracts.ErrorBody) -> None"
    },
    "FRAMED_BODY_DECLARATION_LENGTH_BYTES": {
      "kind": "constant",
      "value": 4
    },
    "FRAMED_BODY_FORMAT": {
      "kind": "constant",
      "value": "riverhog-json-opaque-framing/v1"
    },
    "FRAMED_BODY_MAXIMUM_DECLARATION_BYTES": {
      "kind": "constant",
      "value": 32768
    },
    "FRAMED_BODY_MEDIA_TYPE": {
      "kind": "constant",
      "value": "application/vnd.riverhog.json-opaque-framing"
    },
    "HealthResponse": {
      "kind": "class",
      "schema_sha256": "873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31",
      "signature": "(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None"
    },
    "HttpBodyKind": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "HttpErrorContract": {
      "fields": [
        {
          "default": "required",
          "name": "code",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "status",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "signature": "(code: 'str', status: 'int') -> None"
    },
    "HttpOperationContract": {
      "fields": [
        {
          "default": "required",
          "name": "method",
          "type": "\"Literal['GET', 'POST', 'PUT', 'DELETE', 'PATCH']\""
        },
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "None",
          "name": "request_type",
          "type": "'object | None'"
        },
        {
          "default": "None",
          "name": "response_type",
          "type": "'object | None'"
        },
        {
          "default": "'none'",
          "name": "request_kind",
          "type": "'HttpBodyKind'"
        },
        {
          "default": "'json'",
          "name": "response_kind",
          "type": "'HttpBodyKind'"
        },
        {
          "default": "(200,)",
          "name": "success_statuses",
          "type": "'tuple[int, ...]'"
        },
        {
          "default": "()",
          "name": "errors",
          "type": "'tuple[HttpErrorContract, ...]'"
        },
        {
          "default": "()",
          "name": "path_parameters",
          "type": "'tuple[HttpPathParameterContract, ...]'"
        },
        {
          "default": "()",
          "name": "response_headers",
          "type": "'tuple[HttpResponseHeaderContract, ...]'"
        },
        {
          "default": "<class 'http_api_contracts.ErrorResponse'>",
          "name": "error_type",
          "type": "'type[BaseModel]'"
        }
      ],
      "kind": "class",
      "members": {
        "accepts_error": {
          "kind": "method",
          "signature": "(self, *, status: 'int', code: 'str') -> 'bool'"
        },
        "error_statuses": {
          "kind": "property",
          "signature": "(self) -> 'tuple[int, ...]'"
        },
        "matches": {
          "kind": "method",
          "signature": "(self, method: 'str', path: 'str') -> 'bool'"
        }
      },
      "signature": "(method: \"Literal['GET', 'POST', 'PUT', 'DELETE', 'PATCH']\", path: 'str', request_type: 'object | None' = None, response_type: 'object | None' = None, request_kind: 'HttpBodyKind' = 'none', response_kind: 'HttpBodyKind' = 'json', success_statuses: 'tuple[int, ...]' = (200,), errors: 'tuple[HttpErrorContract, ...]' = (), path_parameters: 'tuple[HttpPathParameterContract, ...]' = (), response_headers: 'tuple[HttpResponseHeaderContract, ...]' = (), error_type: 'type[BaseModel]' = <class 'http_api_contracts.ErrorResponse'>) -> None"
    },
    "HttpOperationErrorAuthority": {
      "kind": "class",
      "members": {
        "accepts": {
          "kind": "method",
          "signature": "(self, operation_id: 'str', *, status: 'int', code: 'str') -> 'bool'"
        },
        "errors_for": {
          "kind": "method",
          "signature": "(self, operation_id: 'str') -> 'tuple[HttpErrorContract, ...]'"
        },
        "from_codes": {
          "kind": "classmethod",
          "signature": "(cls, *, common: 'Collection[str]', operation: 'Mapping[str, Collection[str]]', exact: 'Mapping[str, Collection[str]] | None' = None) -> 'HttpOperationErrorAuthority'"
        },
        "operation_ids": {
          "kind": "property",
          "signature": "(self) -> 'frozenset[str]'"
        }
      },
      "signature": "(*, common: 'Collection[HttpErrorContract]', operation: 'Mapping[str, Collection[HttpErrorContract]]', exact: 'Mapping[str, Collection[HttpErrorContract]] | None' = None) -> 'None'"
    },
    "HttpPathParameterContract": {
      "fields": [
        {
          "default": "required",
          "name": "name",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "value_type",
          "type": "'object'"
        }
      ],
      "kind": "class",
      "signature": "(name: 'str', value_type: 'object') -> None"
    },
    "HttpResponseHeaderContract": {
      "fields": [
        {
          "default": "required",
          "name": "name",
          "type": "'str'"
        },
        {
          "default": "<class 'str'>",
          "name": "value_type",
          "type": "'object'"
        },
        {
          "default": "None",
          "name": "description",
          "type": "'str | None'"
        }
      ],
      "kind": "class",
      "signature": "(name: 'str', value_type: 'object' = <class 'str'>, description: 'str | None' = None) -> None"
    },
    "JSON_SEQUENCE_MEDIA_TYPE": {
      "kind": "constant",
      "value": "application/json-seq"
    },
    "MAX_BROWSE_QUERY_CHARACTERS": {
      "kind": "constant",
      "value": 4096
    },
    "MAX_BROWSE_TOKEN_BYTES": {
      "kind": "constant",
      "value": 8192
    },
    "OperationInterface": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "PUBLIC_ERROR_CODES": {
      "kind": "constant",
      "value": [
        "bad_request",
        "catalog_sync_cursor_expired",
        "catalog_sync_history_expired",
        "catalog_sync_source_changed",
        "catalog_sync_view_changed",
        "conflict",
        "download_allowance_exceeded",
        "forbidden",
        "hash_mismatch",
        "ingress_failed",
        "input_upload_storage_hint_invalid",
        "insufficient_storage",
        "internal_error",
        "invalid_path",
        "invalid_range",
        "invalid_state",
        "invalid_target",
        "job_template_revision_conflict",
        "length_required",
        "method_not_allowed",
        "not_found",
        "precondition_failed",
        "precondition_required",
        "service_unavailable",
        "storage_hint_mismatch",
        "submission_conflict",
        "too_many_active_input_uploads",
        "unauthorized"
      ]
    },
    "QuotedSha256Identity": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "Sha256Identity": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "apply_openapi_error_contract": {
      "kind": "function",
      "signature": "(schema: 'dict[str, Any]', *, operation_error_authority: 'HttpOperationErrorAuthority | None' = None) -> 'dict[str, Any]'"
    },
    "canonical_json_bytes": {
      "kind": "function",
      "signature": "(value: 'object') -> 'bytes'"
    },
    "cursor_feed_operation": {
      "kind": "function",
      "signature": "(*, cursor_parameter: 'str', limit_parameter: 'str | None', fixed_limit: 'int | None' = None) -> 'dict[str, Any]'"
    },
    "error_code_for_status": {
      "kind": "function",
      "signature": "(status: 'int') -> 'str'"
    },
    "error_payload": {
      "kind": "function",
      "signature": "(*, code: 'str', message: 'str', details: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'"
    },
    "error_responses": {
      "kind": "function",
      "signature": "(*codes: 'str') -> 'dict[int | str, dict[str, Any]]'"
    },
    "exact_authority_page_operation": {
      "kind": "function",
      "signature": "(*, authority: 'str', authority_parameter: 'str | None', cursor_parameter: 'str', limit_parameter: 'str | None' = None, fixed_limit: 'int | None' = None) -> 'dict[str, Any]'"
    },
    "exact_set_page_operation": {
      "kind": "function",
      "signature": "(*, authority: 'str', cursor_parameter: 'str', limit_parameter: 'str', validator_header: 'str') -> 'dict[str, Any]'"
    },
    "http_operation_for_request": {
      "kind": "function",
      "signature": "(contracts: 'tuple[HttpOperationContract, ...]', method: 'str', path: 'str') -> 'HttpOperationContract | None'"
    },
    "http_operation_inventory": {
      "kind": "function",
      "signature": "(contracts: 'Sequence[HttpOperationContract]') -> 'list[dict[str, Any]]'"
    },
    "inline_type_schema": {
      "kind": "function",
      "signature": "(value: 'object') -> 'dict[str, Any]'"
    },
    "iter_json_sequence_records": {
      "kind": "function",
      "signature": "(chunks: 'Iterable[bytes]') -> 'Iterator[dict[str, Any]]'"
    },
    "mutable_browse_operation": {
      "kind": "function",
      "signature": "(*, default_page_size: 'int' = 25, maximum_page_size: 'int' = 100) -> 'dict[str, Any]'"
    },
    "operation_interface": {
      "kind": "function",
      "signature": "(value: 'OperationInterface') -> 'dict[str, str]'"
    },
    "operation_openapi": {
      "kind": "function",
      "signature": "(contract: 'HttpOperationContract', *, error_type: 'object | None' = None) -> 'dict[str, Any]'"
    },
    "parse_declared_error_payload": {
      "kind": "function",
      "signature": "(contract: 'HttpOperationContract', *, status: 'int', payload: 'object') -> 'tuple[str, str, dict[str, Any]]'"
    },
    "parse_error_payload": {
      "kind": "function",
      "signature": "(payload: 'object', *, fallback_message: 'str') -> 'tuple[str, str, dict[str, Any]]'"
    },
    "parse_operation_error_payload": {
      "kind": "function",
      "signature": "(authority: 'HttpOperationErrorAuthority', operation_id: 'str', *, status: 'int', payload: 'object') -> 'tuple[str, str, dict[str, Any]]'"
    },
    "parse_quoted_sha256_identity": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    },
    "quote_sha256_identity": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    },
    "safe_http_base_url": {
      "kind": "function",
      "signature": "(value: 'str', *, setting: 'str' = 'base URL', allow_insecure_http: 'bool' = False) -> 'str'"
    },
    "status_for_error_code": {
      "kind": "function",
      "signature": "(code: 'str', *, fallback: 'int' = 500) -> 'int'"
    },
    "structural_model_catalog": {
      "kind": "function",
      "signature": "(contracts: 'Sequence[HttpOperationContract]', *, additional_models: 'Sequence[type[BaseModel]]' = ()) -> 'dict[str, dict[str, Any]]'"
    },
    "validate_browse_query": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    },
    "validate_sha256_identity": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    }
  },
  "module": "http_api_contracts"
}
```
