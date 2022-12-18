package main

import (
	"fmt"

	"github.com/jrouviere/minikv/sstable"
)

func main() {
	err := sstable.WriteFile("./cities.sst", data)
	if err != nil {
		panic(err)
	}

	sst, err := sstable.Load("./cities.sst")
	if err != nil {
		panic(err)
	}

	fmt.Print(sst.Debug())
}

var data = map[string]string{
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
}
