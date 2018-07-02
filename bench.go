// Copyright 2018 Zurich University of Applied Sciences.
// All rights reserved. Use of this source code is governed
// by a BSD-style license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	//"fmt"
	//"io"
	"log"
	"os"
	"time"
	"math/rand"

	//"github.com/json-iterator/go"
	pto3 "github.com/mami-project/pto3-go"
)

type BenchRec struct {
	A pto3.Observation
	B pto3.Observation
}

func randomObs() pto3.Observation {
	var ret pto3.Observation
	
	t := time.Now()
	t2 := t.Add(time.Duration(rand.Int()))

	ret.TimeStart = &t
	ret.TimeEnd = &t2
	ret.Path = &pto3.Path{Source:"1.1.1.1", Target:"2.2.2.2",String:"1.1.1.1 * 2.2.2.2."}
	ret.Condition = &pto3.Condition{Name:"hello.world"}
	ret.Value = "value"
	
	return ret
}

func normalizeV1(rec []byte, rawmeta *pto3.RawMetadata, metachan chan<- map[string]interface{}) ([]pto3.Observation, error) {
	var br BenchRec

	if err := json.Unmarshal(rec, &br); err != nil {
		return nil, err
	}
	
	if err := json.Unmarshal(rec, &br); err != nil {
		return nil, err
	}
	
	if err := json.Unmarshal(rec, &br); err != nil {
		return nil, err
	}
	
	if err := json.Unmarshal(rec, &br); err != nil {
		return nil, err
	}

	return []pto3.Observation{br.A, br.B}, nil
}

func genRecords() {
	for i := 0; i < 250; i++ {
		var br BenchRec
		br.A = randomObs()
		br.B = randomObs()
		
		b, err := json.Marshal(br)
		
		if err != nil {
			panic(err.Error())
		}
		
		os.Stdout.Write(b)
		os.Stdout.Write([]byte{10})
	}
}

func main() {
	gen := flag.Bool("gen",false,"Gen?")

	flag.Parse()
	
	if *gen {
		genRecords()
		return
	}

	mdfile := bytes.NewBufferString("{\"_file_type\":\"ndjson\",\"_owner\":\"munt\"}")

	sn := pto3.NewParallelScanningNormalizer("", 4)
	sn.RegisterFiletype("ndjson", bufio.ScanLines, normalizeV1, nil)

	log.Fatal(sn.Normalize(os.Stdin, mdfile, bufio.NewWriter(os.Stdout)))
}
