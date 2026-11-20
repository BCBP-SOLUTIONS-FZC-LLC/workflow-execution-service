# Liveness, readiness and metrics need no identity headers.
GET {{host}}/healthz
HTTP 200

HEAD {{host}}/healthz
HTTP 200

GET {{host}}/readyz
HTTP 200
[Asserts]
jsonpath "$.status" == "OK"

HEAD {{host}}/readyz
HTTP 200

GET {{metrics}}/metrics
HTTP 200
[Asserts]
body contains "# TYPE"
