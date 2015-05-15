package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"matilde/decoder"
	"matilde/s3"
	"strings"
	"sync"
	"xz"

	amzs3 "github.com/goamz/goamz/s3"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func Do(view *bool, bucketName *string, ext *string, del *bool, put *bool, get *bool, path *string) {
	var conn s3.Connection
	err := conn.Connect()
	if err != nil {
		fmt.Printf("error connectiong to aws:%v \n", err)
	} else {
		bucket, err := s3.Getbucket(*bucketName, &conn)
		if err != nil {
			fmt.Printf("error connectiong to aws:%v \n", err)
		} else {
			list, err := bucket.List("", "", "", 1)
			check(err)
			var wg sync.WaitGroup
			fmt.Println(*ext)
			switch {
			case *view:
				for _, item := range list.Contents {
					if len(*ext) < 1 {
						fmt.Printf("%+v \n ", item.Key)
					} else {
						if strings.Contains(item.Key, *ext) {
							fmt.Printf("%+v \n ", item.Key)
						}
					}
				}
			case *del:
				for _, item := range list.Contents {
					switch {
					case len(*ext) < 1 && len(*path) < 1:
						fmt.Printf("Specifiy extension for mass deletion or the path, exiting...")
					case strings.Contains(item.Key, *ext):
						fmt.Printf("item: %+v \n ", item.Key)
						wg.Add(1)
						go func(item string) {
							if *del {
								err := bucket.Del(item)
								if err != nil {
									panic(err)
								}
								fmt.Printf("deleted: %+v \n ", item)
							}
							wg.Done()
						}(item.Key)
					}
				}
				wg.Wait()
				// delete file at path
				if len(*ext) < 0 && len(*path) > 1 {
					err := bucket.Del(*path)
					if err != nil {
						panic(err)

					}
					fmt.Printf("deleted: %+v \n ", *path)
				}
			}
			switch {
			case *put:
				options := amzs3.Options{}
				data, err := ioutil.ReadFile(*path)
				wg.Add(1)
				go func() {
					err = bucket.Put(*path, data, "", amzs3.Private, options)
					wg.Done()
				}()
				check(err)
				wg.Wait()
			case *get:
				data, err := bucket.Get(*path)
				if err != nil {
					panic(fmt.Sprintf("can't download %v \n ", err))
				}
				fmt.Println("Downloaded \n")
				localpath := fmt.Sprintf("./dowload.xz")
				err = ioutil.WriteFile(localpath, data, 0666)
				check(err)
				reader, err := xz.XzReader(localpath, true)
				check(err)
				decoder.DecodeFromReader(reader)
			}
		}
	}
}

func main() {
	view := flag.Bool("view", false, "print all")
	bucketName := flag.String("bucket", "eta-events-msgpack", "name of bucket")
	ext := flag.String("ext", "", "print ext")
	del := flag.Bool("delete", false, "delete items matching ext")
	put := flag.Bool("put", false, "test writing by putting dummy file")
	get := flag.Bool("get", false, "get file")
	path := flag.String("path", "", "file to get")
	flag.Parse()
	Do(view, bucketName, ext, del, put, get, path)
}
