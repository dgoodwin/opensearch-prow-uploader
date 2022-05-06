package gcsscanner

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/html"
)

// Original code borrowed from https://github.com/vrutkovs/kaas/

const (
	charset              = "abcdefghijklmnopqrstuvwxyz"
	randLength           = 8
	gcsLinkToken         = "gcsweb"
	gcsPrefix            = "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com"
	storagePrefix        = "https://storage.googleapis.com"
	artifactsPath        = "artifacts"
	mustGatherPath       = "must-gather.tar"
	mustGatherFolderPath = "gather-must-gather"
	e2ePrefix            = "e2e"
)

func GetLinksFromURL(url string) ([]string, error) {
	links := []string{}

	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}
	resp, err := netClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %v", url, err)
	}
	defer resp.Body.Close()

	z := html.NewTokenizer(resp.Body)
	for {
		tt := z.Next()

		switch {
		case tt == html.ErrorToken:
			// End of the document, we're done
			return links, nil
		case tt == html.StartTagToken:
			t := z.Token()

			isAnchor := t.Data == "a"
			if isAnchor {
				for _, a := range t.Attr {
					if a.Key == "href" {
						links = append(links, a.Val)
						break
					}
				}
			}
		}
	}
}

func GetMatchingLinkFromURL(baseURL string, regex *regexp.Regexp) (*url.URL, error) {
	urls, err := GetMatchingLinksFromURL(baseURL, []*regexp.Regexp{regex})
	if err != nil {
		return nil, err
	}
	// We're expecting exactly one in this function call, anything else is an error:
	if len(urls) != 1 {
		return nil, fmt.Errorf("expected 1 matching URL, found %d on: %s", len(urls), baseURL)
	}
	return urls[0], nil
}

func GetMatchingLinksFromURL(baseURL string, regexes []*regexp.Regexp) ([]*url.URL, error) {
	allLinks, err := GetLinksFromURL(baseURL)
	if err != nil {
		return []*url.URL{}, fmt.Errorf("failed to fetch links on %s: %v", baseURL, err)
	}
	if len(allLinks) == 0 {
		return []*url.URL{}, fmt.Errorf("no links found on: %s", baseURL)
	}

	matchedLinks := []string{}
	for _, link := range allLinks {
		log.WithField("link", link).Debug("checking link")
		linkSplitBySlash := strings.Split(link, "/")
		lastPathSegment := linkSplitBySlash[len(linkSplitBySlash)-1]
		if len(lastPathSegment) == 0 {
			lastPathSegment = linkSplitBySlash[len(linkSplitBySlash)-2]
		}
		for _, re := range regexes {
			if re.Match([]byte(link)) {
				log.WithField("link", link).Debug("found link match")
				matchedLinks = append(matchedLinks /*gcsPrefix+*/, link)
			}
		}
	}

	matchedURLs := make([]*url.URL, len(matchedLinks))
	for i, ml := range matchedLinks {
		matchedLink := ml
		if strings.HasPrefix(ml, "/") {
			matchedLink = gcsPrefix + ml
		}
		mURL, err := url.Parse(matchedLink)
		if err != nil {
			return []*url.URL{}, fmt.Errorf("failed to parse URL from link %s: %v", ml, err)
		}
		matchedURLs[i] = mURL

	}
	return matchedURLs, nil
}

func ensureMustGatherURL(url string) (int, error) {
	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}
	resp, err := netClient.Get(url)
	if resp == nil {
		return 0, err
	}
	return resp.StatusCode, err
}

/*
func getMustGatherTar(url string) ([]string, error) {
	log.Debug(fmt.Sprintf("Fetching %s", url))
	// Ensure initial URL is valid
	statusCode, err := ensureMustGatherURL(url)
	if err != nil || statusCode != http.StatusOK {
		return []string{}, fmt.Errorf("failed to fetch url %s: code %d, %s", url, statusCode, err)
	}

	fileURL, err := getTarURLFromProw(url)
	if err != nil {
		return []string{}, err
	}
	expectedMustGatherURL := fileURL

	log.Debug(fmt.Sprintf("Found must-gather archive at %s", expectedMustGatherURL))

	// Check that must-gather archive can be fetched and it non-null
	log.Debug("Checking if must-gather archive can be fetched")
	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}
	resp, err := netClient.Head(expectedMustGatherURL)
	if err != nil {
		return []string{}, fmt.Errorf("failed to fetch %s: %v", expectedMustGatherURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return []string{}, fmt.Errorf("failed to check archive at %s: returned %s", expectedMustGatherURL, resp.Status)
	}

	contentLength := resp.Header.Get("content-length")
	if contentLength == "" {
		return []string{}, fmt.Errorf("failed to check archive at %s: no content length returned", expectedMustGatherURL)
	}
	length, err := strconv.Atoi(contentLength)
	if err != nil {
		return []string{}, fmt.Errorf("failed to check archive at %s: invalid content-length: %v", expectedMustGatherURL, err)
	}
	if length == 0 {
		return []string{}, fmt.Errorf("failed to check archive at %s: archive is empty", expectedMustGatherURL)
	}
	return fileURL, nil
}

*/
