package gcsscanner

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Scanner locates files in the artifacts Google Cloud Storage sub-buckets for a given prow job URL.
type Scanner struct{}

func (g *Scanner) FindMatchingFiles(baseURL string, filenameRegexes []*regexp.Regexp) ([]string, error) {
	foundFiles := []string{}

	/*
		prowToplinks, err := GetLinksFromURL(baseURL)
		if err != nil {
			return []string{}, fmt.Errorf("failed to find links at %s: %v", prowToplinks, err)
		}
		if len(prowToplinks) == 0 {
			return []string{}, fmt.Errorf("no links found at %s", baseURL)
		}

		gcsTempURL := ""
		for _, link := range prowToplinks {
			log.WithField("link", link).Debug("found link on prow job")
			if strings.Contains(link, gcsLinkToken) {
				gcsTempURL = link
				break
			}
		}
		if gcsTempURL == "" {
			return []string{}, fmt.Errorf("failed to find GCS link in %v", prowToplinks)
		}
		gcsURL, err := url.Parse(gcsTempURL)
		if err != nil {
			return []string{}, fmt.Errorf("failed to parse GCS URL %s: %v", gcsTempURL, err)
		}
	*/
	// Find the link to gcs artifacts on the prow job page:
	gcsURL, err := GetMatchingLinkFromURL(baseURL, regexp.MustCompile(".*gcsweb.*"))
	if err != nil {
		return []string{}, err
	}
	log.WithField("gcsURL", gcsURL).Info("found GCS URL")

	artifactsURL, err := GetMatchingLinkFromURL(gcsURL.String(), regexp.MustCompile(".*/artifacts/$"))
	if err != nil {
		return []string{}, err
	}
	log.WithField("artifactsURL", artifactsURL).Info("found artifacts URL")

	/*
		// check that 'artifacts' folder is present:
		gcsToplinks, err := GetLinksFromURL(gcsURL.String())
		if err != nil {
			return []string{}, fmt.Errorf("failed to fetch top-level GCS link at %s: %v", gcsURL, err)
		}
		if len(gcsToplinks) == 0 {
			return []string{}, fmt.Errorf("no top-level GCS links at %s found", gcsURL)
		}
		tmpArtifactsURL := ""
		for _, link := range gcsToplinks {
			if strings.HasSuffix(link, "artifacts/") {
				tmpArtifactsURL = gcsPrefix + link
				break
			}
		}
		if tmpArtifactsURL == "" {
			return []string{}, fmt.Errorf("failed to find artifacts link in %v", gcsToplinks)
		}
		artifactsURL, err := url.Parse(tmpArtifactsURL)
		if err != nil {
			return []string{}, fmt.Errorf("failed to parse artifacts link %s: %v", tmpArtifactsURL, err)
		}

	*/

	// Get a list of folders and find those which contain e2e, looking for the top level bucket for the job
	// i.e. e2e-gcp-ovn-upgrade
	artifactLinksToplinks, err := GetLinksFromURL(artifactsURL.String())
	if err != nil {
		return []string{}, fmt.Errorf("failed to fetch artifacts link at %s: %v", gcsURL, err)
	}
	if len(artifactLinksToplinks) == 0 {
		return []string{}, fmt.Errorf("no artifact links at %s found", gcsURL)
	}
	tmpE2eURL := ""
	for _, link := range artifactLinksToplinks {
		log.WithField("link", link).Debug("found link")
		linkSplitBySlash := strings.Split(link, "/")
		lastPathSegment := linkSplitBySlash[len(linkSplitBySlash)-1]
		if len(lastPathSegment) == 0 {
			lastPathSegment = linkSplitBySlash[len(linkSplitBySlash)-2]
		}
		log.Debugf("lastPathSection: %s", lastPathSegment)
		if strings.Contains(lastPathSegment, e2ePrefix) {
			tmpE2eURL = gcsPrefix + link
			break
		}
	}
	if tmpE2eURL == "" {
		return []string{}, fmt.Errorf("failed to find e2e link in %v", artifactLinksToplinks)
	}
	e2eURL, err := url.Parse(tmpE2eURL)
	if err != nil {
		return []string{}, fmt.Errorf("failed to parse e2e link %s: %v", tmpE2eURL, err)
	}

	log.WithField("e2eURL", e2eURL).Info("found e2e link")

	// Support new-style jobs - look for gather-extra
	var gatherMustGatherURL *url.URL

	e2eToplinks, err := GetLinksFromURL(e2eURL.String())
	if err != nil {
		return []string{}, fmt.Errorf("failed to fetch artifacts link at %s: %v", e2eURL, err)
	}
	if len(e2eToplinks) == 0 {
		return []string{}, fmt.Errorf("no top links at %s found", e2eURL)
	}
	for _, link := range e2eToplinks {
		log.WithField("link", link).Debug("found link")
		linkSplitBySlash := strings.Split(link, "/")
		lastPathSegment := linkSplitBySlash[len(linkSplitBySlash)-1]
		if len(lastPathSegment) == 0 {
			lastPathSegment = linkSplitBySlash[len(linkSplitBySlash)-2]
		}
		log.Debugf("lastPathSection: %s", lastPathSegment)
		if lastPathSegment == mustGatherFolderPath {
			tmpMustGatherURL := gcsPrefix + link
			gatherMustGatherURL, err = url.Parse(tmpMustGatherURL)
			if err != nil {
				return []string{}, fmt.Errorf("failed to parse e2e link %s: %v", tmpE2eURL, err)
			}
			break
		}
	}

	if gatherMustGatherURL != nil {
		e2eToplinks, err = GetLinksFromURL(gatherMustGatherURL.String())
		if err != nil {
			return []string{}, fmt.Errorf("failed to fetch gather-must-gather link at %s: %v", e2eURL, err)
		}
		if len(e2eToplinks) == 0 {
			return []string{}, fmt.Errorf("no top links at %s found", e2eURL)
		}
		for _, link := range e2eToplinks {
			log.WithField("link", link).Debug("found link")
			linkSplitBySlash := strings.Split(link, "/")
			lastPathSegment := linkSplitBySlash[len(linkSplitBySlash)-1]
			if len(lastPathSegment) == 0 {
				lastPathSegment = linkSplitBySlash[len(linkSplitBySlash)-2]
			}
			log.Debugf("lastPathSection: %s", lastPathSegment)
			if lastPathSegment == artifactsPath {
				tmpGatherExtraURL := gcsPrefix + link
				gatherMustGatherURL, err = url.Parse(tmpGatherExtraURL)
				if err != nil {
					return []string{}, fmt.Errorf("failed to parse e2e link %s: %v", tmpE2eURL, err)
				}
				break
			}
		}
		e2eURL = gatherMustGatherURL
	}

	gcsMustGatherURL := fmt.Sprintf("%s%s", e2eURL.String(), mustGatherPath)
	tempMustGatherURL := strings.Replace(gcsMustGatherURL, gcsPrefix+"/gcs", storagePrefix, -1)
	expectedMustGatherURL, err := url.Parse(tempMustGatherURL)
	if err != nil {
		return []string{}, fmt.Errorf("failed to parse must-gather link %s: %v", tempMustGatherURL, err)
	}
	mustGatherURL := expectedMustGatherURL.String()
	foundFiles = append(foundFiles, mustGatherURL)
	return foundFiles, nil
}
