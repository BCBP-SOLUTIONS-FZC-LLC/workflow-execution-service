# Inbound event routes, called as the system caller. Event ids are fixed per run so a repeat is a duplicate.
POST {{host}}/api/v1/internal/events/user-profile
{"id":"018f2d3c-0000-7000-8000-01{{run}}","type":"UserDeleted","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{"user_id":"018f2d3c-0000-7000-8000-02{{run}}","deleted_at":"2026-01-02T03:04:05Z"}}
HTTP 200

POST {{host}}/api/v1/internal/events/user-profile
{"id":"018f2d3c-0000-7000-8000-01{{run}}","type":"UserDeleted","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{"user_id":"018f2d3c-0000-7000-8000-02{{run}}","deleted_at":"2026-01-02T03:04:05Z"}}
HTTP 200

POST {{host}}/api/v1/internal/events/user-profile
{"id":"018f2d3c-0000-7000-8000-03{{run}}","type":"NoSuchEvent","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 200

POST {{host}}/api/v1/internal/events/delegation
{"id":"018f2d3c-0000-7000-8000-04{{run}}","type":"NoSuchEvent","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 200

POST {{host}}/api/v1/internal/events/tenant
{"id":"018f2d3c-0000-7000-8000-05{{run}}","type":"NoSuchEvent","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 200

POST {{host}}/api/v1/internal/events/workflow-task
{"id":"018f2d3c-0000-7000-8000-06{{run}}","type":"NoSuchEvent","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 200

POST {{host}}/api/v1/internal/events/no-such-subpath
{"id":"018f2d3c-0000-7000-8000-07{{run}}","type":"UserDeleted","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 404
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "NOT_FOUND"

POST {{host}}/api/v1/internal/events/user-profile
{"id":"018f2d3c-0000-7000-8000-08{{run}}","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 400
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "BAD_REQUEST"
jsonpath "$.trace_id" exists

POST {{host}}/api/v1/internal/events/user-profile
{"id":"018f2d3c-0000-7000-8000-09{{run}}","type":"UserDeleted","source":"hurl","tenant_id":"{{tenant}}","data":{}}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/events/user-profile
{"id":"not-a-uuid","type":"UserDeleted","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{"user_id":"018f2d3c-0000-7000-8000-02{{run}}","deleted_at":"2026-01-02T03:04:05Z"}}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/events/user-profile
{"id":"018f2d3c-0000-7000-8000-10{{run}}","type":"UserDeleted","source":"hurl","tenant_id":"not-a-uuid","time":"2026-01-02T03:04:05Z","data":{"user_id":"018f2d3c-0000-7000-8000-02{{run}}","deleted_at":"2026-01-02T03:04:05Z"}}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/events/user-profile
Content-Type: text/plain
`not an envelope`
HTTP 415
[Asserts]
jsonpath "$.code" == "UNSUPPORTED_MEDIA_TYPE"
