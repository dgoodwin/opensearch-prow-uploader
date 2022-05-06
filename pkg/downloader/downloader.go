package downloader

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

func DownloadFile(dir string, url *url.URL) error {
	log.WithField("url", url).Info("downloading file")
	baseFilename := filepath.Base(url.Path)
	fullFilepath := filepath.Join(dir, baseFilename)
	out, err := os.Create(fullFilepath)
	if err != nil {
		return err
	}
	defer out.Close()
	resp, err := http.Get(url.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bytesWritten, err := io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"sizeKB": bytesWritten / 1024,
		"dest":   fullFilepath,
	}).Info("download complete")
	return nil
}
