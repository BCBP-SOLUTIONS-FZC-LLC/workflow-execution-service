# Identity and role checks. Run without the shared identity headers: every request states its own.
GET {{host}}/api/v1/instances
HTTP 401
[Asserts]
jsonpath "$.status" == 401
jsonpath "$.error" exists

GET {{host}}/api/v1/instances
X-Tenant-Id: {{tenant}}
HTTP 401

GET {{host}}/api/v1/instances
X-User-Id: {{user}}
HTTP 401

GET {{host}}/api/v1/instances
X-Tenant-Id: not-a-uuid
X-User-Id: {{user}}
X-Tenant-Roles: tenant_admin
HTTP 401

GET {{host}}/api/v1/instances
X-Tenant-Id: {{tenant}}
X-User-Id: not-a-uuid
X-Tenant-Roles: tenant_admin
HTTP 401

GET {{host}}/api/v1/tasks
HTTP 401

POST {{host}}/api/v1/instances/018f2d3c-0000-7000-8000-00000000dead/pause
{"record_version":1}
HTTP 401

GET {{host}}/api/v1/instances
X-Tenant-Id: {{tenant}}
X-User-Id: {{user}}
X-Tenant-Roles: viewer
HTTP 200

POST {{host}}/api/v1/instances/018f2d3c-0000-7000-8000-00000000dead/pause
X-Tenant-Id: {{tenant}}
X-User-Id: {{user}}
X-Tenant-Roles: viewer
{"record_version":1}
HTTP 403
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "FORBIDDEN"

GET {{host}}/api/v1/workflows/active-by-user
X-Tenant-Id: {{tenant}}
X-User-Id: {{user}}
X-Tenant-Roles: viewer
[QueryStringParams]
user_id: {{user}}
HTTP 403
[Asserts]
jsonpath "$.code" == "FORBIDDEN"

POST {{host}}/api/v1/internal/events/user-profile
{"id":"018f2d3c-0000-7000-8000-11{{run}}","type":"NoSuchEvent","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 401
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "UNAUTHORIZED"
jsonpath "$.trace_id" exists
jsonpath "$.request_id" exists

POST {{host}}/api/v1/internal/events/user-profile
X-User-Id: {{user}}
X-Tenant-Roles: tenant_admin
{"id":"018f2d3c-0000-7000-8000-12{{run}}","type":"NoSuchEvent","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 401
[Asserts]
jsonpath "$.code" == "UNAUTHORIZED"

POST {{host}}/api/v1/internal/events/user-profile
X-Tenant-Roles: iam-system
{"id":"018f2d3c-0000-7000-8000-13{{run}}","type":"NoSuchEvent","source":"hurl","tenant_id":"{{tenant}}","time":"2026-01-02T03:04:05Z","data":{}}
HTTP 401
[Asserts]
jsonpath "$.code" == "UNAUTHORIZED"

POST {{host}}/api/v1/internal/connector-tasks/018f2d3c-0000-7000-8000-00000000dead/complete
X-Tenant-Id: {{tenant}}
X-User-Id: {{user}}
X-Tenant-Roles: tenant_admin
{"tenant_id":"{{tenant}}"}
HTTP 401
[Asserts]
jsonpath "$.code" == "UNAUTHORIZED"

GET {{host}}/api/v1/internal/workflows/delegate-impact
HTTP 401
[Asserts]
jsonpath "$.code" == "UNAUTHORIZED"
