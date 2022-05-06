package uploader

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type MonitorEvent struct {
	Level   string    `json:"level"`
	Locator string    `json:"locator"`
	Message string    `json:"message"`
	From    time.Time `json:"from"`
	To      time.Time `json:"to"`
	File    string    `json:"file"`
	ProwJob string    `json:"prowJob"`
}

type MonitorEventList struct {
	Items []MonitorEvent `json:"items"`
}

func ParseAndUpload(prowJobID, fp string) error {
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
		err := parseAndUploadMonitorEvents(prowJobID, fp, byteValue)
		if err != nil {
			return err
		}
		log.WithField("file", fp).Info("successfully parsed and uploaded json")
	}
	return nil
}

func parseAndUploadMonitorEvents(prowJobID, fp string, byteValue []byte) error {
	var items MonitorEventList
	err := json.Unmarshal(byteValue, &items)
	if err != nil {
		return err
	}
	for _, item := range items.Items {
		// Add in some additional fields before uploading to opensearch:
		item.File = filepath.Base(fp)
		item.ProwJob = prowJobID
		log.Infof("got item: %v", item)
	}
	return nil
}
