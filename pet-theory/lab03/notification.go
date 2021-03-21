package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// GCSEvent has only a few fields from the event, that we're
// interested in.
type GCSEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
	// PubSubGCSNotification.Message.Data contains other fields,
	// that we ignore.
}

// PubSubGCSNotification matches the JSON payload of the notification
// provided by GCS via PubSub, for a "new file created" event.
type PubSubGCSNotification struct {
	Message struct {
		Attributes  map[string]interface{} `json:"attributes"`
		MessageID   string                 `json:"messageId"`
		PublishTime time.Time              `json:"publishTime"`
		// Data is a base64 encoded GCSEvent
		Data string `json:"data"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

func (notif PubSubGCSNotification) decodeGCSEvent() (GCSEvent, error) {
	decoded, err1 := base64.StdEncoding.DecodeString(notif.Message.Data)
	if err1 != nil {
		return GCSEvent{}, fmt.Errorf("decoding notification message base64 encoded data %q: %v", notif.Message.Data, err1)
	}
	var fileEvent GCSEvent
	err2 := json.Unmarshal(decoded, &fileEvent)
	if err2 != nil {
		return GCSEvent{}, fmt.Errorf("unmarshalling GCSEvent data: %v. Could not parse %q", err2, string(decoded))
	}
	return fileEvent, nil
}

func readBody(r *http.Request) (GCSEvent, error) {
	log.Println("Reading POST data")
	body, err1 := ioutil.ReadAll(r.Body)
	if err1 != nil {
		return GCSEvent{}, fmt.Errorf("Error reading POST data: %v", err1)
	}
	log.Println("POST data =", string(body))
	var notification PubSubGCSNotification
	var file GCSEvent
	r.Body.Close()
	if len(bytes.TrimSpace(body)) == 0 {
		return GCSEvent{}, fmt.Errorf("Empty request body. Expecting a PubSub notification of a GCS event.")
	}
	err2 := json.Unmarshal(body, &notification)
	if err2 != nil {
		return GCSEvent{}, fmt.Errorf("Error unmarshalling POST data: %v. Could not parse %q", err2, string(body))
	}
	file, err3 := notification.decodeGCSEvent()
	if err3 != nil {
		return GCSEvent{}, fmt.Errorf("Error extracting notification encoded data: %v", err3)
	}
	return file, nil
}
