package glue

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type statusGetter struct{ status types.SchemaVersionStatus }

func (g statusGetter) GetSchemaVersion(_ context.Context, _ *awsglue.GetSchemaVersionInput, _ ...func(*awsglue.Options)) (*awsglue.GetSchemaVersionOutput, error) {
	return &awsglue.GetSchemaVersionOutput{SchemaVersionId: aws.String(goodVersionID), Status: g.status}, nil
}

func TestCodec_Encode_RefusesASchemaVersionThatIsNotAvailable(t *testing.T) {
	for _, status := range []types.SchemaVersionStatus{
		types.SchemaVersionStatusPending, types.SchemaVersionStatusFailure, types.SchemaVersionStatusDeleting, "",
	} {
		t.Run(string(status), func(t *testing.T) {
			c := newTestCodec(statusGetter{status: status})

			_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

			require.Error(t, err)
			assert.ErrorContains(t, err, "not AVAILABLE")
		})
	}
}

func TestCodec_Encode_AcceptsAnAvailableSchemaVersion(t *testing.T) {
	c := newTestCodec(statusGetter{status: types.SchemaVersionStatusAvailable})

	_, versionID, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

	require.NoError(t, err)
	assert.Equal(t, goodVersionID, versionID)
}

func TestCodec_Decode_RefusesAHeaderWithNoPayloadAndACompressedPayload(t *testing.T) {
	c := newTestCodec(nil)
	headerOnly := make([]byte, glueHeaderSize)
	headerOnly[0] = glueHeaderVersion
	compressed := append(append([]byte{}, headerOnly...), []byte(`{}`)...)
	compressed[1] = 0x05

	for name, encoded := range map[string][]byte{"header only": headerOnly, "compressed": compressed} {
		t.Run(name, func(t *testing.T) {
			_, err := c.Decode(context.Background(), "any", encoded)

			require.Error(t, err)
		})
	}
}
