package uploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go"
	log "github.com/sirupsen/logrus"
)

type Uploader struct {
	OpenSearchClient *opensearch.Client
	User             string
	Pass             string
}

// Types from origin monitorapi package

type Condition struct {
	Level string

	Locator string
	Message string
}

type EventInterval struct {
	Condition
	OpenSearchMetadata

	From time.Time
	To   time.Time
}

// OpenSearchMetadata are values we inject into the intervals from origin.
type OpenSearchMetadata struct {
	File     string
	ProwJob  string
	Duration string
}

type EventIntervalList struct {
	Items []EventInterval
}

type EventLevel int

const (
	Info EventLevel = iota
	Warning
	Error
)

func (e EventLevel) String() string {
	switch e {
	case Info:
		return "Info"
	case Warning:
		return "Warning"
	case Error:
		return "Error"
	default:
		panic(fmt.Sprintf("did not define event level string for %d", e))
	}
}

func EventLevelFromString(s string) (EventLevel, error) {
	switch s {
	case "Info":
		return Info, nil
	case "Warning":
		return Warning, nil
	case "Error":
		return Error, nil
	default:
		return Error, fmt.Errorf("did not define event level string for %q", s)
	}

}

func (u *Uploader) ParseAndUpload(prowJobID, fp string) error {
	jsonFile, err := os.Open(fp)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}

	if strings.HasPrefix(filepath.Base(fp), "e2e-events_") {
		err := u.parseAndUploadMonitorEvents(prowJobID, fp, byteValue)
		if err != nil {
			return err
		}
		log.WithField("file", fp).Info("successfully parsed and uploaded json")
	}
	return nil
}

func (u *Uploader) parseAndUploadMonitorEvents(prowJobID, fp string, byteValue []byte) error {

	allIntervals := EventIntervalList{}
	err := json.Unmarshal(byteValue, &allIntervals)
	if err != nil {
		return err
	}

	log.WithField("intervals", len(allIntervals.Items)).Info("parsed json intervals")

	// We will bulk post 1000 at a time.
	chunk := make([][]byte, 0, 1000)
	totalUploaded := 0

	//_, err := jsonparser.ArrayEach(byteValue, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
	for i := range allIntervals.Items {

		// Add some additional values before we submit to ElasticSearch:
		allIntervals.Items[i].File = filepath.Base(fp)
		allIntervals.Items[i].ProwJob = prowJobID

		// inject a duration in seconds, this will be used for the opensearch gantt chart:
		dur := allIntervals.Items[i].To.Sub(allIntervals.Items[i].From)
		durInt := int64(math.Round(dur.Seconds()))
		allIntervals.Items[i].Duration = fmt.Sprintf("%ds", durInt)

		// We need one json document per line to use the bulk
		// index API, so we unmarshal and marshal to let go clean it up. This is adding a bit of slowness we could
		// otherwise avoid.
		finalBytes, err2 := json.Marshal(allIntervals.Items[i])
		if err != nil {
			log.WithError(err2).Error("error marshalling json")
			return err
		}
		log.Debugf("finalBytes: %s", finalBytes)

		// Add to our chunk array, if we're at 1000 it's time to submit and re-init the array.
		chunk = append(chunk, finalBytes)
		if len(chunk) >= 1000 {
			err2 := u.bulkIndex(prowJobID, chunk)
			totalUploaded += len(chunk)
			if err2 != nil {
				log.WithError(err2).Info("error submitting bulk request")
				return err2
			}
			// Re-initialize the array.
			chunk = make([][]byte, 0, 1000)
		}
	}

	// Upload the remaining chunk:
	err2 := u.bulkIndex(prowJobID, chunk)
	if err2 != nil {
		log.WithError(err2).Info("error submitting bulk request")
		return nil
	}
	totalUploaded += len(chunk)

	log.WithField("uploaded", totalUploaded).Info("finished upload")

	return nil
}

func (u *Uploader) bulkIndex(prowJobID string, chunk [][]byte) error {
	var bbuf bytes.Buffer

	log.WithField("count", len(chunk)).Info("bulk uploading documents")

	for _, docLine := range chunk {
		indexLine := fmt.Sprintf("{\"index\":{\"_index\":\"%s\"}}\n", prowJobID)
		bbuf.Write([]byte(indexLine))
		bbuf.Write(docLine)
		bbuf.Write([]byte("\n"))
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	req, err := http.NewRequest("POST",
		"https://search-trt-opensearch-test-m4gt2sys3kyzqeqauf4fr27x7u.us-east-1.es.amazonaws.com/_bulk", &bbuf)
	req.SetBasicAuth(u.User, u.Pass)
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	/*
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		bodyString := string(bodyBytes)
		log.Info(bodyString)
	*/

	log.WithField("status", resp.Status).Info("bulk request made")
	return nil
}
