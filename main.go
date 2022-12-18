package main

import (
	"fmt"
	"time"

	"github.com/jrouviere/minikv/db"
)

func main() {
	store, err := db.New("./data/")
	if err != nil {
		panic(err)
	}

	store.Set("deleted", "wrong")

	i := 0
	for k, v := range data1 {
		store.Set(k, v)

		i++
		if i%10 == 0 {
			if err := store.Flush(); err != nil {
				panic(err)
			}
		}
	}
	store.Set("inmemory", "true")
	store.Delete("deleted")

	check(store, "Cairo")
	check(store, "Osaka")
	check(store, "Mumbai")
	check(store, "Beijing")
	check(store, "Tokyo")
	check(store, "Aaa")
	check(store, "Fff")
	check(store, "Zzz")
	check(store, "Paris")
	check(store, "London")
	check(store, "inmemory")
	check(store, "deleted")

	if err := store.Flush(); err != nil {
		panic(err)
	}

	if err := store.MergeAll(); err != nil {
		panic(err)
	}

	check(store, "Cairo")
	check(store, "Paris")
	check(store, "London")
	check(store, "inmemory")
	check(store, "deleted")
	check(store, "Zzz")
}

func check(store *db.DB, key string) {
	start := time.Now()

	val := store.Get(key)
	if val == "" {
		fmt.Printf("%s: not found [%v]\n", key, time.Since(start))
	} else {
		fmt.Printf("%s: %s [%v]\n", key, val, time.Since(start))
	}
}

var data1 = map[string]string{
	"Tokyo":          "37,468,000",
	"Delhi":          "28,514,000",
	"Shanghai":       "25,582,000",
	"São Paulo":      "21,650,000",
	"Mexico City":    "21,581,000",
	"Cairo":          "20,076,000",
	"Mumbai":         "19,980,000",
	"Beijing":        "19,618,000",
	"Dhaka":          "19,578,000",
	"Osaka":          "19,281,000",
	"New York":       "18,819,000",
	"Karachi":        "15,400,000",
	"Buenos Aires":   "14,967,000",
	"Chongqing":      "14,838,000",
	"Istanbul":       "14,751,000",
	"Kolkata":        "14,681,000",
	"Manila":         "13,482,000",
	"Lagos":          "13,463,000",
	"Rio de Janeiro": "13,293,000",
	"Tianjin":        "13,215,000",
	"Kinshasa":       "13,171,000",
	"Guangzhou":      "12,638,000",
	"Los Angeles":    "12,458,000",
	"Moscow":         "12,410,000",
	"Shenzhen":       "11,908,000",
	"Lahore":         "11,738,000",
	"Bangalore":      "11,440,000",
	"Paris":          "10,901,000",
	"Bogotá":         "10,574,000",
	"Jakarta":        "10,517,000",
	"Chennai":        "10,456,000",
	"Lima":           "10,391,000",
	"Bangkok":        "10,156,000",
	"Seoul":          "9,963,000",
	"Nagoya":         "9,507,000",
	"Hyderabad":      "9,482,000",
	"London":         "9,046,000",
	"Tehran":         "8,896,000",
	"Chicago":        "8,864,000",
	"Chengdu":        "8,813,000",
}
