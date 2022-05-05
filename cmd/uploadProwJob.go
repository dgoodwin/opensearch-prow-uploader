package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

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
	},
}

func init() {
	rootCmd.AddCommand(uploadProwJobCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// uploadProwJobCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// uploadProwJobCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
