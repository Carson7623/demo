package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type BgtLink struct {
	List []BgtLinkInfo `json:"list"`
}

type BgtLinkInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Link    string `json:"link"`
}

const GETURL = "http://rj.gxjyzy.com/download.html"

func main() {
	nameArr := make(map[int]string)
	versionArr := make(map[int]string)
	linkArr := make(map[int]string)
	bgtLink := new(BgtLink)
	res, err := getContent(GETURL)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer res.Body.Close()
	if res.StatusCode != 200 {
		fmt.Println("读取内容失败")
		return
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		fmt.Println("获取内容失败-1")
		return
	}

	doc.Find(".con span.con-title").Each(func(i int, s *goquery.Selection) {
		title := s.Text()
		nameArr[i] = title
	})

	doc.Find(".con span.banbenhao").Each(func(i int, s *goquery.Selection) {
		title := s.Text()
		versionArr[i] = title
	})

	res, err = getContent(GETURL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		fmt.Println("读取内容失败")
		return
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
	}
	allContent := string(body)
	allContent = strings.Replace(allContent, " ", "", -1)  //得到的内容去除空格符
	allContent = strings.Replace(allContent, "\n", "", -1) //得到的内容去除换行符

	searchArr := []string{"tpc", "tpad", "tmobile"}
	for key, val := range searchArr {
		contentArr := findDestStr(allContent, "#"+val+"'\\).click\\(function\\(\\)\\{window.location.href='(.*?)'")
		linkArr[key] = contentArr
	}

	for i := 0; i < len(nameArr); i++ {
		bgtInfo := new(BgtLinkInfo)
		bgtInfo.Name = nameArr[i]
		bgtInfo.Version = versionArr[i]
		bgtInfo.Link = linkArr[i]
		bgtLink.List = append(bgtLink.List, *bgtInfo)
	}

	fmt.Println(bgtLink)
}

func getContent(url string) (*http.Response, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		return res, err
	}
	return res, nil
}

func findDestStr(src, regular string) string {
	compileRegex := regexp.MustCompile(regular)
	matchArr := compileRegex.FindStringSubmatch(src)

	if len(matchArr) > 0 {
		return matchArr[len(matchArr)-1]
	}
	return ""
}
