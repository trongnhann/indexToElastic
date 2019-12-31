package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"os"
	"strconv"
	"strings"
	"time"
)

const URL = "http://192.168.5.178:9200"
func indexingWithBulk(index []Indexes, docs []string, URL string, curr int) int {
	fmt.Println("Version Elastic: ", elastic.Version)

	// Context to call API
	ctx := context.Background()

	// Init Client
	client, err := elastic.NewClient(
		elastic.SetSniff(true),
		elastic.SetURL(URL),
		)

	if err != nil {
		fmt.Println("elastic.NewClient() ERROR:", err)
	}
	bulk := client.Bulk()

	// Do data có nhiều Type nên tách mỗi type thành 1 index
	tmp_curr := curr
	for i:= curr; i< len(docs); i++ {
		tmp_curr ++
		fmt.Println("index vs i ===== ",i ,index[i].Index.Id )
		indexName := index[i].Index.Type

		// Kiểm tra index đã tồn tại hay chưa
		indices := []string{indexName}
		existService := elastic.NewIndicesExistsService(client)
		existService.Index(indices)
		exist, err := existService.Do(ctx)

		if err != nil {
			fmt.Println("existService.Do(ctx) ERROR:", err)
		} else if exist == false { // Nếu chưa tồn tại thì khởi tạo
			fmt.Println("Oh no! The index ", indexName, " doesn't exist.")
			fmt.Println("Creating the index............")

			// Tạo index
			createIndex, err := client.CreateIndex(indexName).Do(ctx)
			if err != nil {
				fmt.Println("client.CreateIndex(indexName).Do(ctx) ERROR = ", err)
			}
			if !createIndex.Acknowledged {
				// Not acknowledged
			}

			fmt.Println("Created the index ", indexName)
		}
		// tạo bulk Client

		idStr := strconv.Itoa(index[i].Index.Id)

		// Tạo các bulk req
		req := elastic.NewBulkIndexRequest()
		req.OpType("index") // Loại req là "index" document
		req.Index(indexName)
		//req.Type("_doc")
		req.Id(idStr)
		req.Doc(docs[i])
		// Thông tin req
		fmt.Println("req:", req)

		// Thêm req vào bulk
		bulk = bulk.Add(req)
	}

	// Do() sends requests to Elasticsearch
	bulkResp, err := bulk.Do(ctx)

	if err != nil {
		fmt.Println("bulk.Do(ctx) ERROR:", err)
	} else {
		indexed := bulkResp.Indexed()
		fmt.Println("bulkResp.Indexed():", indexed)
		for _, info := range indexed {
			fmt.Println("Bulk response Index:", info)
		}
	}
	return tmp_curr
}

type Indexes struct {
	Index Index `json: "index"`
}

type Index struct {
	Index	string `json:	"index"`
	Type	string `json:	"type"`
	Id	int `json:	"id"`
}

type Docs struct {
	LineID   int `json:"line_id"`
	PlayName  string `json:"play_name"`
	SpeechNumber   int `json:"speech_number"`
	LineNumber   string `json:"line_number"`
	Speaker   string `json:"speaker"`
	TextEntry   string `json:"text_entry"`
}

func callIndex(fileName string)  {
	jsonFile, err := os.Open(fileName)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Mở file thành công ")
	defer jsonFile.Close()

	if err != nil {
		fmt.Println("Lỗi read json")
	}

	scanner := bufio.NewScanner(jsonFile)
	countScanner := 0

	var docs []string
	var indexes []Indexes
	curr := 0
	for scanner.Scan() {
		// Nếu countScanner là số chẵn là dòng lưu thông tin index
		// Ta sẽ parse chúng thành struct để lấy thông tin
		tmp := scanner.Text()
		if countScanner % 2 == 0 {
			var index Indexes
			// không thể parse json với trường bắt đầu khác chữ cái
			tmp = strings.ReplaceAll(tmp,"_","")
			//tmp := `{"index":{"_index":"shakespeare","_type":"scene","_id":1}}`
			//tmp := `{"index":{"index":"shakespeare","type":"scene","id":1}} `
			fmt.Println("Thông tin index: ", tmp)

			err := json.Unmarshal([]byte(tmp), &index)

			if err != nil {
				fmt.Println("json.Unmarshal([]byte(tmp), &index) ERROR: ", err)
				break
			} else {
				fmt.Println("Thông tin index sau khi parse: ", index)
			}
			indexes = append(indexes, index)
		} else {
			docs = append(docs, tmp)
		}
		countScanner++

		if len(docs) % 500 == 0  && len(docs) != 0{ // Cứ 10 doc index 1 lần

			fmt.Println("================================ Before", curr)
			curr = indexingWithBulk(indexes, docs, URL, curr)
			fmt.Println("================================ After", curr)

		}
	}
	indexingWithBulk(indexes, docs, URL, curr)
}

func main()  {
	start := time.Now()

	callIndex("test.json")

	elapsed := time.Since(start)

	fmt.Println("Time took: ", elapsed)

}