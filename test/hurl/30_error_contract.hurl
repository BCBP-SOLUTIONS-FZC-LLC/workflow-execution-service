# Every refusal is a problem document carrying code, trace_id and request_id.
GET {{host}}/api/v1/no-such-route
HTTP 404
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "NOT_FOUND"
jsonpath "$.status" == 404
jsonpath "$.type" == "https://errors.bcbp.io/execution/not-found"
jsonpath "$.instance" == "/api/v1/no-such-route"
jsonpath "$.trace_id" exists
jsonpath "$.request_id" exists

DELETE {{host}}/api/v1/instances
HTTP 405
[Asserts]
header "Content-Type" contains "application/problem+json"
header "Allow" contains "GET"
header "Allow" contains "POST"
jsonpath "$.code" == "METHOD_NOT_ALLOWED"
jsonpath "$.trace_id" exists
jsonpath "$.request_id" exists

POST {{host}}/api/v1/instances
Content-Type: text/plain
`subject_id=hurl`
HTTP 415
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "UNSUPPORTED_MEDIA_TYPE"

POST {{host}}/api/v1/instances
Content-Type: application/json
`{not json`
HTTP 400
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "BAD_REQUEST"
jsonpath "$.trace_id" exists
jsonpath "$.request_id" exists

POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"contract-{{run}}","workflow_version_id":"not-a-uuid"}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"
jsonpath "$.invalid_params[0].name" == "workflow_version_id"

POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"contract-{{run}}","workflow_version_id":"{{version_id}}","surprise":1}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"
jsonpath "$.invalid_params[0].name" == "surprise"

POST {{host}}/api/v1/instances
{"subject_id":"x","business_key":"contract-{{run}}","workflow_version_id":"{{version_id}}"}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"contract-{{run}}","workflow_version_id":"018f2d3c-0000-7000-8000-00000000dead"}
HTTP 404
[Asserts]
jsonpath "$.code" == "VERSION_NOT_FOUND"

GET {{host}}/api/v1/instances/not-a-uuid
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

GET {{host}}/api/v1/instances/018f2d3c-0000-7000-8000-00000000dead
HTTP 404
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "INSTANCE_NOT_FOUND"
jsonpath "$.trace_id" exists
jsonpath "$.request_id" exists

POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"contract-{{run}}","workflow_version_id":"{{version_id}}"}
HTTP 201
[Captures]
instance_id: jsonpath "$.id"

# The same request again is a replay and returns the live instance.
POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"contract-{{run}}","workflow_version_id":"{{version_id}}"}
HTTP 201
[Asserts]
jsonpath "$.id" == {{instance_id}}

# A different context under the same business key is a conflict.
POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"contract-{{run}}","workflow_version_id":"{{version_id}}","context_json":{"other":1}}
HTTP 409
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "DUPLICATE_BUSINESS_KEY"

POST {{host}}/api/v1/instances/{{instance_id}}/pause
{"record_version":999999}
HTTP 409
[Asserts]
jsonpath "$.code" == "RECORD_VERSION_CONFLICT"

POST {{host}}/api/v1/instances/{{instance_id}}/cancel
{"reason":"   ","record_version":1}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/instances
{"subject_id":"hurl-nul-\u0000-{{run}}","business_key":"contract-{{run}}","workflow_version_id":"{{version_id}}"}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

GET {{host}}/api/v1/instances?limit=99999999999999999999
HTTP 200
[Asserts]
jsonpath "$.items" exists

POST {{host}}/api/v1/instances/{{instance_id}}/terminate
{"reason":"contract scenario cleanup"}
HTTP 200
[Asserts]
jsonpath "$.message" == "action completed"
