package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const BASEURL = "https://clinicaltrials.gov/api/v2/studies"

var cancerList = []string{"chagas disease", "trachoma"}

type studyResponseForToken struct {
	NextPageToken string `json:"nextPageToken"`
}

func main() {
	ch := make(chan string, len(cancerList))
	for _, condition := range cancerList {
		createPossibleDirectory := fmt.Sprintf("./data/raw/%s", strings.ReplaceAll(condition, " ", "_"))
		os.MkdirAll(createPossibleDirectory, 0755)
		go func(cond string) {
			err := fetchAllPagesAndSave(BASEURL, cond)
			if err != nil {
				ch <- fmt.Sprintf("error in %s: %v", cond, err)
				return
			}
			ch <- fmt.Sprintf("%s", cond)
		}(condition)
	}
	for range cancerList {
		response := <-ch
		fmt.Println(response)
	}
}

func fetchAllPagesAndSave(baseurl, condition string) error {
	pageToken := ""
	page := 0
	for {
		body, nextPageToken, err := sendRequest(baseurl, condition, pageToken)
		if err != nil {
			return err
		}
		err = saveRawPages(condition, body, page)
		if err != nil {
			return err
		}
		if nextPageToken == "" {
			break
		}
		pageToken = nextPageToken
		page++
	}
	return nil
}

func sendRequest(baseurl, condition, pageToken string) (string, string, error) {
	u, err := url.Parse(baseurl)
	if err != nil {
		return "", "", err
	}
	params := url.Values{}
	params.Add("query.cond", condition)
	if pageToken != "" {
		params.Add("pageToken", pageToken)
	}
	u.RawQuery = params.Encode()
	res, err := http.Get(u.String())
	if err != nil {
		return "", "", err
	}
	defer res.Body.Close()
	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return "", "", err
	}
	var parsed studyResponseForToken
	err = json.Unmarshal(bytes, &parsed)
	if err != nil {
		return "", "", err
	}
	return string(bytes), parsed.NextPageToken, nil
}

func saveRawPages(condition, body string, page int) error {
	localization := fmt.Sprintf("./data/raw/%s/%s-%v.json", strings.ReplaceAll(condition, " ", "_"), strings.ReplaceAll(condition, " ", "_"), page)
	err := os.WriteFile(localization, []byte(body), 0644)
	if err != nil {
		return err
	}
	return nil
}
