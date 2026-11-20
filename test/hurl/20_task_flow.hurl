# Start an instance and drive its human task: list, read, refuse a claim and a stale completion, complete it.
POST {{host}}/api/v1/instances
{"subject_id":"hurl-subject-{{run}}","business_key":"task-flow-{{run}}","workflow_version_id":"{{version_id}}"}
HTTP 201
[Captures]
instance_id: jsonpath "$.id"

GET {{host}}/api/v1/tasks
[Options]
retry: 15
retry-interval: 1s
[QueryStringParams]
instance_id: {{instance_id}}
HTTP 200
[Captures]
task_id: jsonpath "$.items[0].id"
record_version: jsonpath "$.items[0].record_version"
[Asserts]
jsonpath "$.items" count == 1
jsonpath "$.items[0].node_key" == "Design/Task_prep"
jsonpath "$.items[0].workflow_instance_id" == {{instance_id}}
jsonpath "$.items[0].department_id" == "f14abc9f-befa-54ff-b57f-c15ab3090062"

GET {{host}}/api/v1/tasks/{{task_id}}
HTTP 200
[Asserts]
jsonpath "$.id" == {{task_id}}
jsonpath "$.assignments" count >= 1
jsonpath "$.assignments[0].user_id" == {{user}}
jsonpath "$.assignments[0].assigned_at" exists

GET {{host}}/api/v1/tasks/018f2d3c-0000-7000-8000-00000000dead
HTTP 404
[Asserts]
header "Content-Type" contains "application/problem+json"
jsonpath "$.code" == "TASK_NOT_FOUND"

POST {{host}}/api/v1/tasks/{{task_id}}/claim
{"record_version":{{record_version}}}
HTTP 409
[Asserts]
jsonpath "$.code" == "CLAIM_NOT_APPLICABLE"

POST {{host}}/api/v1/tasks/{{task_id}}/complete
{"record_version":0}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/tasks/{{task_id}}/complete
{"record_version":{{record_version}},"result_json":[1]}
HTTP 400
[Asserts]
jsonpath "$.code" == "BAD_REQUEST"

POST {{host}}/api/v1/tasks/{{task_id}}/complete
{"record_version":999999,"result_json":{"approved":true}}
HTTP 409
[Asserts]
jsonpath "$.code" == "RECORD_VERSION_CONFLICT"

POST {{host}}/api/v1/tasks/{{task_id}}/complete
{"record_version":{{record_version}},"result_json":{"approved":true,"notes":"scenario run {{run}}"}}
HTTP 202
[Asserts]
jsonpath "$.id" == {{task_id}}

GET {{host}}/api/v1/tasks/{{task_id}}
[Options]
retry: 15
retry-interval: 1s
HTTP 200
[Asserts]
jsonpath "$.status" == "COMPLETED"
jsonpath "$.completed_at" exists

GET {{host}}/api/v1/instances/{{instance_id}}
[Options]
retry: 15
retry-interval: 1s
HTTP 200
[Asserts]
jsonpath "$.status" == "COMPLETED"

GET {{host}}/api/v1/workflows/active-by-user
[QueryStringParams]
user_id: {{user}}
HTTP 200
[Asserts]
body not contains "{{task_id}}"
