package cmd

import (
	"crypto/tls"
	"net/http"

	"github.com/opensearch-project/opensearch-go"
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
		run()
	},
}

func run() {
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{"https://search-dgoodwin-test-o4g3tsj6smjnfyxybu4m67ospy.us-east-1.es.amazonaws.com"},
		Username:  opts.Username, // For testing only. Don't store credentials in code.
		Password:  opts.Password,
	})
	if err != nil {
		log.WithError(err).Fatal("error connecting to opensearch")
	}

	// Print OpenSearch version information on console.
	log.Info(client.Info())
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
