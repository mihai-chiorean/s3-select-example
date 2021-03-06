package csvdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
)

// Filter is a key/value pair used for the where clause.
// could have used
type Filter struct {
	k, v string
}

// NewFilter creates a new filter foa key value pair
func NewFilter(k, v string) Filter {
	return Filter{k, v}
}

// Row is one record in the csv
type Row struct {
	ID        string `json:"policyID"`
	StateCode string `json:"statecode"`
	County    string `json:"county"`
}

// Client represents the struct used to make queries to an s3 csv
type Client struct {
	s3iface.S3API
	// S3 path - prefix + file/resource name
	key    string
	bucket string
}

// NewClient instantiates a new client struct
func NewClient(s s3iface.S3API, bucket, key string) *Client {
	return &Client{
		S3API:  s,
		bucket: bucket,
		key:    key,
	}
}

// QueryRawContext uses a raw string query to call s3select on a resource
func (c *Client) QueryRawContext(ctx context.Context, query string, useHeader bool) ([]map[string]string, error) {
	csvInput := &s3.CSVInput{}
	if useHeader {
		// query using header names. This is a choice for this example
		// many csv files do not have a header row; In that case,
		// this property would not be needed and the "filters" would be
		// on the column index (e.g. _1, _2, _3...)
		csvInput.SetFileHeaderInfo("Use")
	}
	req := &s3.SelectObjectContentInput{
		Bucket:         aws.String(c.bucket),
		Key:            aws.String(c.key),
		Expression:     aws.String(query),
		ExpressionType: aws.String("SQL"),
		InputSerialization: &s3.InputSerialization{
			CSV: csvInput,
		},
	}

	// we want the output as json, to have the field names in it too
	req = req.SetOutputSerialization(&s3.OutputSerialization{
		JSON: &s3.JSONOutput{},
	})
	out, err := c.SelectObjectContentWithContext(ctx, req)
	if err != nil {
		return nil, err
	}

	stream := out.GetEventStream()
	defer stream.Close()

	rows := []map[string]string{}
	buf := []byte{}
	for v := range stream.Events() {
		if err := stream.Err(); err != nil {
			return nil, err
		}

		switch v.(type) {
		case *s3.RecordsEvent:
			rec, _ := v.(*s3.RecordsEvent)
			buf = append(buf, rec.Payload...)
		default:
		}
	}
	rawRows := bytes.Split(buf, []byte("\n"))
	for _, v := range rawRows {
		if len(v) <= 0 {
			continue
		}
		var row map[string]string
		if err := json.Unmarshal(v, &row); err != nil {
			return nil, errors.Wrapf(err, "unable to parse json: %s", string(v))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// QueryContext is used to make a select query agains the CSV in S3
func (c *Client) QueryContext(ctx context.Context, filters ...Filter) ([]map[string]string, error) {

	if len(filters) <= 0 {
		return nil, fmt.Errorf("nothing to query by")
	}

	// a little logic to add tttthe "and" keyword between where clauses
	q := "select * from s3object s where "
	for i, p := range filters {
		q += fmt.Sprintf("s.%s = '%s'", p.k, p.v)
		if i < len(filters)-1 {
			q += " and "
		}
	}

	req := &s3.SelectObjectContentInput{
		Bucket:         aws.String(c.bucket),
		Key:            aws.String(c.key),
		Expression:     aws.String(q),
		ExpressionType: aws.String("SQL"),
		InputSerialization: &s3.InputSerialization{
			CSV: &s3.CSVInput{
				// query using header names. This is a choice for this example
				// many csv files do not have a header row; In that case,
				// this property would not be needed and the "filters" would be
				// on the column index (e.g. _1, _2, _3...)
				FileHeaderInfo: aws.String("Use"),
			},
		},
	}

	// we want the output as json, to have the field names in it too
	req = req.SetOutputSerialization(&s3.OutputSerialization{
		JSON: &s3.JSONOutput{},
	})
	out, err := c.SelectObjectContentWithContext(ctx, req)
	if err != nil {
		return nil, err
	}

	stream := out.GetEventStream()
	defer stream.Close()

	rows := []map[string]string{}
	for v := range stream.Events() {
		if err := stream.Err(); err != nil {
			return nil, err
		}

		switch v.(type) {
		case *s3.RecordsEvent:
			rec, _ := v.(*s3.RecordsEvent)
			var row map[string]string
			if err := json.Unmarshal(rec.Payload, &row); err != nil {
				return nil, errors.Wrapf(err, "unable to parse json: %s", string(rec.Payload))
			}
			rows = append(rows, row)
		default:
		}
	}

	return rows, nil
}
