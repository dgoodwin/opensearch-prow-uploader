package uploader

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type Uploader struct {
	OpenSearchClient *opensearch.Client
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
	bulkIndexer, err := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Index:         prowJobID,
		Client:        u.OpenSearchClient, // The Elasticsearch client
		NumWorkers:    2,                  // 1 for now, our free tier server gets cranky with concurrent
		FlushBytes:    int(1000000),       // The flush threshold in bytes
		FlushInterval: 10 * time.Second,   // The periodic flush interval
	})
	_, err = jsonparser.ArrayEach(byteValue, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		// Add some additional values:
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
		//fmt.Println(string(newValue))

		// Add an item to the BulkIndexer
		//
		err = bulkIndexer.Add(
			context.Background(),
			opensearchutil.BulkIndexerItem{
				Index: prowJobID,
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",

				// DocumentID is the (optional) document ID
				//DocumentID: strconv.Itoa(a.ID),

				// Body is an `io.Reader` with the payload
				Body: bytes.NewReader(newValue),

				// OnSuccess is called for each successful operation
				OnSuccess: func(ctx context.Context, item opensearchutil.BulkIndexerItem, res opensearchutil.BulkIndexerResponseItem) {
					log.Info("successful upload")
					fmt.Println("successful upload")
				},

				// OnFailure is called for each failed operation
				OnFailure: func(ctx context.Context, item opensearchutil.BulkIndexerItem, res opensearchutil.BulkIndexerResponseItem, err error) {
					fmt.Printf("failed upload err: %s", err)
					fmt.Printf("failed upload res: %+v", res)
					if err != nil {
						log.WithError(err).Error("bulk indexer error")
					} else {
						log.WithField("error", res.Error.Reason).Error("bulk indexer error")
					}
				},
			},
		)
		if err != nil {
			log.WithError(err).Error("error adding to bulk indexer")
			return
		}
	}, "items")
	if err != nil {
		return err
	}
	log.Infof("bulk indexer stats: %+v", bulkIndexer.Stats())
	log.Info("closing indexer")
	if err := bulkIndexer.Close(context.Background()); err != nil {
		log.WithError(err).Error("error closing bulk indexer")
	}
	log.Infof("bulk indexer stats: %+v", bulkIndexer.Stats())

	/*
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
	*/
	return nil
}
