/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mihai-chiorean/s3-select-example/client/csvdb"
	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	flagBucket  string = "bucket"
	flagKey     string = "key"
	flagHeaders string = "use-headers"
	flagRegion  string = "region"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "s3ql",
	Short: "A small cli to run/test sql queries against a CSV in AWS S3",
	Long:  ``,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		bucket, err := cmd.Flags().GetString(flagBucket)
		if err != nil {
			log.Fatal(err)
		}
		key, err := cmd.Flags().GetString(flagKey)
		if err != nil {
			log.Fatal(err)
		}
		useHeader, err := cmd.Flags().GetBool(flagHeaders)
		if err != nil {
			log.Fatal(err)
		}
		region, err := cmd.Flags().GetString(flagRegion)
		if err != nil {
			log.Fatal(err)
		}
		if len(bucket) == 0 || len(key) == 0 {
			log.Fatal("--bucket and --resource are required flags")
		}
		query := strings.Join(args, " ")
		logger, _ := zap.NewDevelopment()
		defer logger.Sync()
		log := logger.Sugar()
		log.Info(query)
		log.Info(args)
		log.Info(os.Args)
		// instantiate s3 session and client
		sess := s3.New(session.New(), aws.NewConfig().WithRegion(region))
		cli := csvdb.NewClient(sess, bucket, key)
		rows, err := cli.QueryRawContext(context.Background(), query, useHeader)
		if err != nil {
			log.Fatal(err)
		}
		for _, v := range rows {
			buf, err := json.Marshal(v)
			if err != nil {
				log.Error(err)
				continue
			}
			log.Info(string(buf))
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	//rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.s3ql.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP(flagHeaders, "t", true, "Tells S3 Select that the csv has a header row and to use it in the query.")
	rootCmd.Flags().StringP(flagBucket, "b", "", "S3 bucket where the data resides")
	rootCmd.Flags().StringP(flagKey, "k", "", "The S3 resource - prefix/key - of the CSV file")
	rootCmd.Flags().StringP(flagRegion, "r", "", "The AWS region where the bucket is")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".s3ql" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".s3ql")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
func main() {
	Execute()
}
