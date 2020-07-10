package csvdb

import (
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

type selectObjectContentRequest struct {
	XMLName            xml.Name `xml:"SelectObjectContentRequest"`
	Text               string   `xml:",chardata"`
	Xmlns              string   `xml:"xmlns,attr"`
	Expression         string   `xml:"Expression"`
	ExpressionType     string   `xml:"ExpressionType"`
	InputSerialization struct {
		Text string `xml:",chardata"`
		CSV  struct {
			Text           string `xml:",chardata"`
			FileHeaderInfo string `xml:"FileHeaderInfo"`
		} `xml:"CSV"`
	} `xml:"InputSerialization"`
	OutputSerialization struct {
		Text string `xml:",chardata"`
		JSON string `xml:"JSON"`
	} `xml:"OutputSerialization"`
}

func TestQueryContext(t *testing.T) {
	tests := map[string]struct {
		expQuery     string
		input        []Filter
		expErr       string
		selectCalled bool
	}{
		"no input": {
			input:  []Filter{},
			expErr: "nothing to query by",
		},
		"select single field": {
			input: []Filter{
				NewFilter("policyID", "1"),
			},
			expQuery:     fmt.Sprintf("select * from s3object s where s.policyID = '1'"),
			selectCalled: true,
		},
		"select multiple fields": {
			input: []Filter{
				NewFilter("policyID", "1"),
				NewFilter("statecode", "FL"),
			},
			expQuery:     fmt.Sprintf("select * from s3object s where s.policyID = '1' and s.statecode = 'FL'"),
			selectCalled: true,
		},
	}
	for n, test := range tests {
		t.Run(n, func(t *testing.T) {
			// fake S3 server
			s3Server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				// Used on client initialization
				if r.Method == "HEAD" {
					rw.WriteHeader(200)
					return
				}

				// check the query to S3
				body, err := ioutil.ReadAll(r.Body)
				assert.NoError(t, err)
				r.Header.Get("Content-Type")

				var req selectObjectContentRequest
				assert.NoError(t, xml.Unmarshal(body, &req))
				assert.Contains(t, req.Expression, test.expQuery)

				// send response
				rw.Write([]byte(`{"policyID":"2","statecode":"FL","county":""}`))
			}))
			defer s3Server.Close()

			var sess = session.Must(session.NewSession(&aws.Config{
				Region:     aws.String("mock-region"),
				SleepDelay: func(time.Duration) {},
			}))
			svc := s3.New(sess, &aws.Config{
				Endpoint:         aws.String(s3Server.URL),
				DisableSSL:       aws.Bool(true),
				S3ForcePathStyle: aws.Bool(true),
			})
			c := NewClient(svc, "bucket", "key")
			assert.NotNil(t, c)

			_, err := c.QueryContext(context.Background(), test.input...)
			if len(test.expErr) > 0 {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.expErr)
			}
		})
	}
}
