// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const exampleMetaJSON = `{
	"ulid": "%s",
	"minTime": 1,
	"maxTime": 1000,
	"stats": {
		"numSamples": 500,
		"numSeries": 5,
		"numChunks": 5
	},
	"compaction": {
		"level": 1,
		"sources": [
			"%s"
		]
	},
	"version": 1,
	"thanos": {
		"labels": {
			"ext1": "val1"
		},
		"downsample": {
			"resolution": 124
		},
		"source": "test",
		"files": [
			{
				"rel_path": "chunks/000001",
				"size_bytes": 3751
			},
			{
				"rel_path": "index",
				"size_bytes": 401
			},
			{
				"rel_path": "meta.json"
			}
		]
	}
}
`

func TestMeta_ReadWrite(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-meta")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	id := ulid.MustNew(1, nil)
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(tmpDir, "meta.json"), []byte(fmt.Sprintf(exampleMetaJSON, id, id)), os.ModePerm))
	m, err := Read(filepath.Join(tmpDir))
	testutil.Ok(t, err)

	testutil.Equals(t, Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MinTime: 1,
			MaxTime: 1000,
			Stats: tsdb.BlockStats{
				NumSeries:  5,
				NumSamples: 500,
				NumChunks:  5,
			},
			Compaction: tsdb.BlockMetaCompaction{
				Level:   1,
				Sources: []ulid.ULID{id},
			},
			Version: 1,
		},
		Thanos: Thanos{
			Labels: map[string]string{"ext1": "val1"},
			Downsample: ThanosDownsample{
				Resolution: 124,
			},
			Source: "test",
			Files: []File{
				{RelPath: "chunks/000001", SizeBytes: 3751},
				{RelPath: "index", SizeBytes: 401},
				{RelPath: "meta.json"},
			},
		},
	}, *m)

	b := bytes.Buffer{}
	testutil.Ok(t, m.Write(&b))
	testutil.Equals(t, fmt.Sprintf(exampleMetaJSON, id, id), b.String())
}
