package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testPort = 8586

func init() {
	// Standardize servlet count for tests so we don't get different
	// results on different machines.
	defaultServletCount = 16

	// Turn off logging.
	log.SetOutput(ioutil.Discard)
}

func sendText(method string, path string, body string) (int, string) {
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, _ := http.NewRequest(method, fmt.Sprintf("http://localhost:%d%s", testPort, path), bytes.NewBufferString(body))
	req.Header.Add("Content-Type", "text/plain")
	resp, err := client.Do(req)
	if err != nil {
		panic("Response failed: " + err.Error())
	}
	defer resp.Body.Close()

	b, _ := ioutil.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func sendJSON(method string, path string, body string) (int, interface{}) {
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, _ := http.NewRequest(method, fmt.Sprintf("http://localhost:%d%s", testPort, path), bytes.NewBufferString(body))
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		panic("Response failed: " + err.Error())
	}
	defer resp.Body.Close()

	var ret interface{}
	json.NewDecoder(resp.Body).Decode(&ret)

	return resp.StatusCode, ret
}

func getJSON(path string) (int, interface{}) {
	return sendJSON("GET", path, "")
}

func postJSON(path string, body string) (int, interface{}) {
	return sendJSON("POST", path, body)
}

func putJSON(path string, body string) (int, interface{}) {
	return sendJSON("PUT", path, body)
}

func patchJSON(path string, body string) (int, interface{}) {
	return sendJSON("PATCH", path, body)
}

func deleteJSON(path string, body string) (int, interface{}) {
	return sendJSON("DELETE", path, body)
}

func assertResponse(t *testing.T, resp *http.Response, statusCode int, content string, message string) {
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != statusCode || content != string(body) {
		t.Fatalf("%v:\nexp:[%v] %v\ngot:[%v] %v.", message, statusCode, content, resp.StatusCode, string(body))
	}
}

func sendTestHttpRequest(method string, url string, contentType string, body string) (*http.Response, error) {
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Add("Content-Type", contentType)
	return client.Do(req)
}

func runTestServerAt(path string, f func(s *Server)) {
	s := NewServer(testPort, path)
	s.Version = "0.0.0"
	go s.ListenAndServe()
	defer s.Close()
	f(s)
}

func runTestServer(f func(s *Server)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)
	runTestServerAt(path, f)
}

func setupTestTable(name string) {
	if code, _ := postJSON("/tables", `{"name":"`+name+`"}`); code != 200 {
		panic("test table fixture error: " + name)
	}
}

func setupTestProperty(tableName string, name string, transient bool, dataType string) {
	path := fmt.Sprintf("/tables/%v/properties", tableName)
	body := fmt.Sprintf(`{"name":"%v", "transient":%v, "dataType":"%v"}`, name, transient, dataType)
	if code, _ := postJSON(path, body); code != 200 {
		panic("test property fixture error: " + tableName + "/" + name)
	}
}

func setupTestData(t *testing.T, tableName string, items [][]string) {
	for _, item := range items {
		status, _ := patchJSON(fmt.Sprintf("/tables/%s/objects/%s/events/%s", tableName, item[0], item[1]), item[2])
		assert.Equal(t, status, 200)
	}
}

func _codegen(t *testing.T, tableName string, query string) {
	resp, _ := sendTestHttpRequest("POST", fmt.Sprintf("http://localhost:8586/tables/%s/query/codegen", tableName), "application/json", query)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func _dumpObject(t *testing.T, tableName string, objectId string) {
	resp, _ := sendTestHttpRequest("GET", fmt.Sprintf("http://localhost:8586/tables/%s/objects/%s/events", tableName, objectId), "application/json", "")
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func jsonenc(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
