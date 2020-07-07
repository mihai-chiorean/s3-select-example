package csvdb

import "context"

// Filter is a key/value pair used for the where clause.
// could have used
type Filter struct {
	k, v string
}

// NewFilter creates a new filter for a key value pair
func NewFilter(k, v string) Filter {
	return Filter{k, v}
}

// Row is one record in the csv
type Row struct{}

// Client represents the struct used to make queries to an s3 csv
type Client struct {
}

// QueryContext is used to make a select query agains the CSV in S3
func (c *Client) QueryContext(ctx context.Context, filters ...Filter) ([]*Row, error) {
	return nil, nil
}
