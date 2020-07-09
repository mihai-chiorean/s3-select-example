package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mihai-chiorean/s3-select-example/client/csvdb"
	"gopkg.in/yaml.v2"
)

type config struct {
	Bucket     string `yaml:"bucket"`
	Resource   string `yaml:"resource"`
	AwsProfile string `yaml:"aws_profile"`
	Region     string `yaml:"region"`
}

func main() {
	buf, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	cfg := config{}
	if err := yaml.Unmarshal(buf, &cfg); err != nil {
		log.Fatal(err)
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: credentials.NewSharedCredentials("", cfg.AwsProfile),
	}))

	s3cli := s3.New(sess)
	cli := csvdb.NewClient(s3cli, cfg.Bucket, cfg.Resource)
	r, err := cli.QueryContext(context.Background(), csvdb.NewFilter("policyID", "119736"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(r[0].StateCode)

}
