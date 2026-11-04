//go:build compat

package compat_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

const awsRegion = "us-east-1"

// The two IAM topics the Workflow-Events Consumer reads, each with its own
// queue subscribed with raw delivery, as the consumer's runbook requires.
var iamTopics = []struct{ topic, queue string }{
	{"iam-membership-events", "membership-workflow-q"},
	{"iam-tenant-events", "tenant-workflow-q"},
}

// bus is LocalStack's SNS and SQS with the consumer reading the queues and
// forwarding to the two services. Publishing goes through platform-events'
// SNS publisher, the path IAM's services publish on.
type bus struct {
	endpoint   string
	publishers map[string]events.Publisher
}

func eventConsumerDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("EVENT_CONSUMER_DIR")
	if dir == "" {
		dir = filepath.Join("..", "..", "..", "event_consumer")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("resolve EVENT_CONSUMER_DIR: %v", err)
	}
	if _, err := os.Stat(filepath.Join(abs, "cmd", "event-consumer")); err != nil {
		t.Fatalf("EVENT_CONSUMER_DIR=%s is not a Workflow-Events Consumer checkout: %v", abs, err)
	}
	return abs
}

func startLocalStack(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "localstack/localstack:3",
			ExposedPorts: []string{"4566/tcp"},
			Env:          map[string]string{"SERVICES": "sns,sqs"},
			WaitingFor:   wait.ForLog("Ready.").WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start LocalStack: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("LocalStack host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4566")
	if err != nil {
		t.Fatalf("LocalStack port: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// startBus creates the topics and queues, writes the consumer's topology and
// runs the consumer binary against both services.
func (e *env) startBus(t *testing.T) *bus {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", awsRegion)
	ctx := context.Background()
	b := &bus{endpoint: startLocalStack(t), publishers: map[string]events.Publisher{}}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(awsRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(b.endpoint) })
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(b.endpoint) })

	queueURLs := map[string]string{}
	for _, tq := range iamTopics {
		topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(tq.topic)})
		if err != nil {
			t.Fatalf("create topic %s: %v", tq.topic, err)
		}
		dlq, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(tq.queue + "-dlq")})
		if err != nil {
			t.Fatalf("create queue %s-dlq: %v", tq.queue, err)
		}
		redrive, err := json.Marshal(map[string]string{"deadLetterTargetArn": queueARN(t, sqsClient, dlq.QueueUrl), "maxReceiveCount": "5"})
		if err != nil {
			t.Fatalf("redrive policy: %v", err)
		}
		queue, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(tq.queue), Attributes: map[string]string{"RedrivePolicy": string(redrive)},
		})
		if err != nil {
			t.Fatalf("create queue %s: %v", tq.queue, err)
		}
		if _, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
			Endpoint:   aws.String(queueARN(t, sqsClient, queue.QueueUrl)),
			Attributes: map[string]string{"RawMessageDelivery": "true"},
		}); err != nil {
			t.Fatalf("subscribe %s: %v", tq.queue, err)
		}
		queueURLs[tq.queue] = aws.ToString(queue.QueueUrl)
		pub, err := events.NewSNSPublisher(events.SNSConfig{TopicARN: aws.ToString(topic.TopicArn), Region: awsRegion, EndpointURL: b.endpoint})
		if err != nil {
			t.Fatalf("SNS publisher %s: %v", tq.topic, err)
		}
		b.publishers[tq.topic] = pub
	}

	topology := fmt.Sprintf(`version: 1
targets:
  - name: execution
    addr: %s
    tokenEnv: EXECUTION_SERVICE_TOKEN
  - name: definition
    addr: %s
    tokenEnv: DEFINITION_SERVICE_TOKEN
queues:
  - name: membership
    url: %s
  - name: tenant
    url: %s
routes:
  DepartmentMembershipRevoked: [definition]
  MembershipRevoked: [definition, execution]
  TenantStateChanged: [execution]
`, e.executionURL, e.definitionURL, queueURLs["membership-workflow-q"], queueURLs["tenant-workflow-q"])
	topologyPath := filepath.Join(t.TempDir(), "topology.yaml")
	if err := os.WriteFile(topologyPath, []byte(topology), 0o600); err != nil {
		t.Fatalf("write topology: %v", err)
	}

	bin := filepath.Join(t.TempDir(), "event-consumer")
	goBuild(t, eventConsumerDir(t), "./cmd/event-consumer", bin)
	health := freePort(t)
	var logs bytes.Buffer
	cmd := exec.Command(bin)
	cmd.Dir = t.TempDir()
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"APP_ENV=dev",
		"AWS_REGION=" + awsRegion,
		"AWS_ENDPOINT_URL=" + b.endpoint,
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_EC2_METADATA_DISABLED=true",
		"TOPOLOGY_CONFIG_PATH=" + topologyPath,
		"HEALTH_PORT=" + strconv.Itoa(health),
		"EXECUTION_SERVICE_TOKEN=" + executionToken,
		"DEFINITION_SERVICE_TOKEN=" + definitionToken,
		"RETRY_BACKOFF_BASE=1s",
		"RETRY_BACKOFF_MAX=2s",
		"OTEL_SDK_DISABLED=true",
	}
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start event consumer: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited
		if t.Failed() {
			t.Logf("event consumer output:\n%s", logs.String())
		}
	})
	deadline := time.Now().Add(60 * time.Second)
	for {
		if resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(health) + "/readyz"); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return b
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("event consumer never became ready:\n%s", logs.String())
		}
		select {
		case err := <-exited:
			exited <- err
			t.Fatalf("event consumer exited before it was ready (%v):\n%s", err, logs.String())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// publish sends one IAM event on its topic through platform-events.
func (b *bus) publish(t *testing.T, topic, eventType string, tn tenant, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", eventType, err)
	}
	env := events.Envelope[json.RawMessage]{
		ID: uuid.NewString(), Type: eventType, Source: "iam-org-membership", SchemaVersion: "1.0",
		TenantID: tn.id.String(), Timestamp: time.Now().UTC(), Payload: data,
	}
	if err := b.publishers[topic].Publish(context.Background(), env); err != nil {
		t.Fatalf("publish %s: %v", eventType, err)
	}
}

func queueARN(t *testing.T, client *sqs.Client, url *string) string {
	t.Helper()
	attrs, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: url, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("queue arn %s: %v", aws.ToString(url), err)
	}
	return attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
}
