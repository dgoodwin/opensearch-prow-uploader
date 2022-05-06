package gcsscanner

import (
	"net/url"
	"regexp"

	log "github.com/sirupsen/logrus"
)

// Scanner locates files in the artifacts Google Cloud Storage sub-buckets for a given prow job URL.
type Scanner struct{}

func (g *Scanner) FindMatchingFiles(baseURL string) ([]*url.URL, error) {
	foundFiles := []*url.URL{}
	// Find the link to gcs artifacts on the prow job page:
	gcsURL, err := GetMatchingLinkFromURL(baseURL, regexp.MustCompile(".*gcsweb.*"), false)
	if err != nil {
		return []*url.URL{}, err
	}
	log.WithField("gcsURL", gcsURL).Info("found GCS URL")

	artifactsURL, err := GetMatchingLinkFromURL(gcsURL.String(), regexp.MustCompile("artifacts"), true)
	if err != nil {
		return []*url.URL{}, err
	}
	log.WithField("artifactsURL", artifactsURL).Info("found artifacts URL")

	// Get a list of folders and find those which contain e2e, looking for the top level bucket for the job
	// i.e. e2e-gcp-ovn-upgrade
	e2eURL, err := GetMatchingLinkFromURL(artifactsURL.String(), regexp.MustCompile(".*e2e.*"), true)
	if err != nil {
		return []*url.URL{}, err
	}
	log.WithField("e2eURL", e2eURL).Info("found e2eURL")

	// Locate gather-extra/artifacts/ for some files:
	/*
		gatherExtraURL, err := GetMatchingLinkFromURL(e2eURL.String(), regexp.MustCompile("gather-extra"), true)
		if err != nil {
			return []*url.URL{}, err
		}
		gatherExtraURL, err = GetMatchingLinkFromURL(gatherExtraURL.String(), regexp.MustCompile("artifacts"), true)
		if err != nil {
			return []*url.URL{}, err
		}
		log.WithField("gatherExtraURL", gatherExtraURL).Info("found gatherExtraURL")
		kubeEventsURL, err := GetMatchingLinkFromURL(gatherExtraURL.String(), regexp.MustCompile("events.json"), true)
		if err != nil {
			return []*url.URL{}, err
		}
		foundFiles = append(foundFiles, kubeEventsURL)
	*/

	// Locate openshift-e2e-test for more files:
	e2eTestFilesURL, err := GetMatchingLinkFromURL(e2eURL.String(), regexp.MustCompile("openshift-e2e-test"), true)
	if err != nil {
		return []*url.URL{}, err
	}
	e2eTestFilesURL, err = GetMatchingLinkFromURL(e2eTestFilesURL.String(), regexp.MustCompile("^artifacts$"), true)
	if err != nil {
		return []*url.URL{}, err
	}
	e2eTestFilesURL, err = GetMatchingLinkFromURL(e2eTestFilesURL.String(), regexp.MustCompile("^junit$"), true)
	if err != nil {
		return []*url.URL{}, err
	}
	e2eTestFiles, err := GetMatchingLinksFromURL(e2eTestFilesURL.String(), []*regexp.Regexp{
		regexp.MustCompile("e2e-events_.*\\.json"),
		//regexp.MustCompile("e2e-intervals_everything_.*\\.json"),
	}, true)
	if err != nil {
		return []*url.URL{}, err
	}
	for _, e2ef := range e2eTestFiles {
		foundFiles = append(foundFiles, e2ef)
	}

	return foundFiles, nil
}
