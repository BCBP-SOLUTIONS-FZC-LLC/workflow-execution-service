# Connector completion callbacks, called as the system caller. Body checks run before the task lookup.
POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/complete
{"tenant_id":"{{tenant}}","output":{"ok":true}}
HTTP 404
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "TASK_NOT_FOUND"
jsonpath "$.trace_id" exists
jsonpath "$.request_id" exists

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/fail
{"tenant_id":"{{tenant}}","error_class":"upstream_timeout"}
HTTP 404
[Asserts]
jsonpath "$.code" == "TASK_NOT_FOUND"

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/fail
{"tenant_id":"{{tenant}}","error_class":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
HTTP 404
[Asserts]
jsonpath "$.code" == "TASK_NOT_FOUND"

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/fail
{"tenant_id":"{{tenant}}","error_class":""}
HTTP 400
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/fail
{"tenant_id":"{{tenant}}","error_class":"   "}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/fail
{"tenant_id":"{{tenant}}"}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/fail
{"tenant_id":"{{tenant}}","error_class":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/complete
{"output":{"ok":true}}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/internal/connector-tasks/not-a-uuid/complete
{"tenant_id":"{{tenant}}"}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

# The internal routes ignore fields they do not know, so this reaches the task lookup.
POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/complete
{"tenant_id":"{{tenant}}","surprise":1}
HTTP 404
[Asserts]
jsonpath "$.code" == "TASK_NOT_FOUND"
