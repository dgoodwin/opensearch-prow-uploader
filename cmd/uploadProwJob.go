package cmd

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/dgoodwin/opensearch-prow-uploader/pkg/downloader"
	"github.com/dgoodwin/opensearch-prow-uploader/pkg/gcsscanner"
	"github.com/dgoodwin/opensearch-prow-uploader/pkg/uploader"
	"github.com/opensearch-project/opensearch-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type UploadOpts struct {
	Username string
	Password string
}

var opts UploadOpts

// uploadProwJobCmd represents the uploadProwJob command
var uploadProwJobCmd = &cobra.Command{
	Use:   "upload-prow-job [prowJobURL]",
	Short: "Upload artifacts from an OpenShift CI prow job to OpenSearch",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			log.Fatal("missing required argument for prowJobURL")
		}
		prowJobURL := args[0]
		log.WithField("prowJob", prowJobURL).Info("uploading prow job")
		err := run(prowJobURL)
		if err != nil {
			log.WithError(err).Fatal("error encountered")
		}
	},
}

func run(prowJobURL string) error {

	prowJobURLTokens := strings.Split(prowJobURL, "/")
	prowJobID := prowJobURLTokens[len(prowJobURLTokens)-1]
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{"https://search-trt-opensearch-test-m4gt2sys3kyzqeqauf4fr27x7u.us-east-1.es.amazonaws.com"},
		Username:  opts.Username, // For testing only. Don't store credentials in code.
		Password:  opts.Password,
	})
	if err != nil {
		return errors.Wrap(err, "error connecting to opensearch")
	}

	// Print OpenSearch version information on console.
	log.Info(client.Info())

	scanner := gcsscanner.Scanner{}
	fileURLs, err := scanner.FindMatchingFiles(prowJobURL)
	if err != nil {
		return err
	}

	dir, err := ioutil.TempDir("", "opensearch-prow-uploader-")
	if err != nil {
		return err
	}
	log.WithField("dir", dir).Info("created temporary directory")
	defer os.RemoveAll(dir)

	for i, fu := range fileURLs {
		// Replace our gcs web UI with storage.googleapis.com for better downloading:
		gcsPrefix := "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com"
		storagePrefix := "https://storage.googleapis.com"
		tempURL := strings.Replace(fu.String(), gcsPrefix+"/gcs", storagePrefix, -1)
		newURL, err := url.Parse(tempURL)
		if err != nil {
			return fmt.Errorf("failed to parse URL from %s: %v", fu, err)
		}
		fileURLs[i] = newURL
		dlfp, err := downloader.DownloadFile(dir, fileURLs[i])
		if err != nil {
			return err
		}

		upl := uploader.Uploader{
			OpenSearchClient: client,
			User:             opts.Username,
			Pass:             opts.Password,
		}
		err = upl.ParseAndUpload(prowJobID, dlfp)
		if err != nil {
			return err
		}
	}
	return nil
}

func init() {
	rootCmd.AddCommand(uploadProwJobCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// uploadProwJobCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	uploadProwJobCmd.Flags().StringVar(&opts.Username, "user", "openshift", "TRT OpenSearch username")
	uploadProwJobCmd.Flags().StringVar(&opts.Password, "pass", "", "TRT OpenSearch password")
}
