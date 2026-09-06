package sqsops

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/zap"
	"strings"
)

const (
	SQSQueueNameParam    = "sqs_queue_name"
	SQSMessageIDParam    = "sqs_msg_id"
	SQSErrorCodeParam    = "sqs_error_code"
	SQSErrorMessageParam = "sqs_error_message"
)

func ReadOrCreateSQSQueueURL(ctx context.Context, sqsClient *sqs.Client, queueName string, logger *zap.Logger) (*string, bool, error) {
	getRes, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	qn := strings.Split(queueName, ".")
	fifo := strings.EqualFold(qn[len(qn)-1], "fifo")

	var queueURL *string
	if err != nil {
		var opError *types.QueueDoesNotExist
		if errors.As(err, &opError) {
			logger.Info("queue doesn't exist. it will be created", zap.String(SQSQueueNameParam, queueName))

			createRes, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(queueName),
				Attributes: map[string]string{
					"FifoQueue": fmt.Sprintf("%t", fifo),
				},
				Tags: nil,
			})

			if err != nil {
				return nil, false, fmt.Errorf("SQS writer: failed to create queue %s: %w", queueName, err)
			}

			queueURL = createRes.QueueUrl
		} else {
			return nil, false, fmt.Errorf("SQS writer: failed to get URL of the queue %s: %w", queueName, err)
		}
	} else {
		queueURL = getRes.QueueUrl
	}

	return queueURL, fifo, nil
}
