package uploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/opensearch-project/opensearch-go"
	log "github.com/sirupsen/logrus"
)

type Uploader struct {
	OpenSearchClient *opensearch.Client
	User             string
	Pass             string
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

	// We will bulk post 1000 at a time.
	chunk := make([][]byte, 0, 1000)

	_, err := jsonparser.ArrayEach(byteValue, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {

		// Add some additional values before we submit to ElasticSearch:
		newValue, err2 := jsonparser.Set(value, []byte("\""+filepath.Base(fp)+"\""), "file")
		if err2 != nil {
			log.WithError(err2).Error("error setting json value")
			return
		}
		newValue, err2 = jsonparser.Set(newValue, []byte("\""+prowJobID+"\""), "prowJob")
		if err2 != nil {
			log.WithError(err2).Error("error setting json value")
			return
		}

		// jsonparser library Set is "experimental" and leaves some the json formatted as it came in, with
		// our new values add in a somewhat clunky fashion. We need one json document per line to use the bulk
		// index API, so we unmarshal and marshal to let go clean it up. This is adding a bit of slowness we could
		// otherwise avoid.
		var temp map[string]interface{}
		if err2 = json.Unmarshal(newValue, &temp); err2 != nil {
			log.WithError(err2).Error("error cleaning up json")
			return
		}
		finalBytes, err2 := json.Marshal(temp)
		if err != nil {
			log.WithError(err2).Error("error cleaning up json")
			return
		}

		// Add to our chunk array, if we're at 1000 it's time to submit and re-init the array.
		chunk = append(chunk, finalBytes)
		if len(chunk) >= 1000 {
			err2 := u.bulkIndex(prowJobID, chunk)
			if err2 != nil {
				log.WithError(err2).Info("error submitting bulk request")
				return
			}
			// Re-initialize the array.
			chunk = make([][]byte, 0, 1000)
		}

		//log.Info(string(finalBytes))
	}, "items")
	if err != nil {
		return err
	}

	// Upload the remaining chunk:
	err2 := u.bulkIndex(prowJobID, chunk)
	if err2 != nil {
		log.WithError(err2).Info("error submitting bulk request")
		return nil
	}

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
		"https://search-dgoodwin-test-o4g3tsj6smjnfyxybu4m67ospy.us-east-1.es.amazonaws.com/_bulk", &bbuf)
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
