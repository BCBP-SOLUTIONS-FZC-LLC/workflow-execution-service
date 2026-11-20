# Start an instance, pause, resume and cancel it. The worker applies each signal after the 202, so reads retry.
POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"lifecycle-{{run}}","workflow_version_id":"{{version_id}}"}
HTTP 201
[Captures]
instance_id: jsonpath "$.id"
[Asserts]
jsonpath "$.status" == "RUNNING"
jsonpath "$.workflow_version_id" == {{version_id}}
jsonpath "$.tenant_id" == {{tenant}}
jsonpath "$.business_key" == "lifecycle-{{run}}"

# The same request again is a replay and returns the live instance.
POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"lifecycle-{{run}}","workflow_version_id":"{{version_id}}"}
HTTP 201
[Asserts]
jsonpath "$.id" == {{instance_id}}

# A different context under the same business key is a conflict.
POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"lifecycle-{{run}}","workflow_version_id":"{{version_id}}","context_json":{"other":1}}
HTTP 409
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "DUPLICATE_BUSINESS_KEY"

GET {{host}}/api/v1/instances
[QueryStringParams]
subject_id: hurl-subject-{{run}}
HTTP 200
[Asserts]
jsonpath "$.items" count == 1
jsonpath "$.items[0].id" == {{instance_id}}

GET {{host}}/api/v1/instances/{{instance_id}}
[Options]
retry: 15
retry-interval: 1s
HTTP 200
[Captures]
record_version: jsonpath "$.record_version"
[Asserts]
jsonpath "$.status" == "RUNNING"
jsonpath "$.tasks" count == 1
jsonpath "$.tasks[0].node_key" == "Design/Task_prep"

GET {{host}}/api/v1/instances/{{instance_id}}/events
HTTP 200
[Asserts]
jsonpath "$.items" count >= 1

POST {{host}}/api/v1/instances/{{instance_id}}/pause
{"record_version":{{record_version}},"reason":"scenario pause"}
HTTP 202
[Asserts]
jsonpath "$.message" == "signal accepted"

GET {{host}}/api/v1/instances/{{instance_id}}
[Options]
retry: 15
retry-interval: 1s
HTTP 200
[Captures]
record_version: jsonpath "$.record_version"
[Asserts]
jsonpath "$.status" == "PAUSED"

POST {{host}}/api/v1/instances/{{instance_id}}/pause
{"record_version":{{record_version}}}
HTTP 409
[Asserts]
jsonpath "$.code" == "INVALID_INSTANCE_STATE"

POST {{host}}/api/v1/instances/{{instance_id}}/resume
{"record_version":{{record_version}}}
HTTP 202

GET {{host}}/api/v1/instances/{{instance_id}}
[Options]
retry: 15
retry-interval: 1s
HTTP 200
[Captures]
record_version: jsonpath "$.record_version"
[Asserts]
jsonpath "$.status" == "RUNNING"

POST {{host}}/api/v1/instances/{{instance_id}}/cancel
{"reason":"scenario cancel","record_version":{{record_version}}}
HTTP 202

GET {{host}}/api/v1/instances/{{instance_id}}
[Options]
retry: 15
retry-interval: 1s
HTTP 200
[Asserts]
jsonpath "$.status" == "TERMINATED"

POST {{host}}/api/v1/instances/{{instance_id}}/cancel
{"reason":"scenario cancel again","record_version":1}
HTTP 409
[Asserts]
jsonpath "$.code" == "INSTANCE_ALREADY_TERMINAL"
