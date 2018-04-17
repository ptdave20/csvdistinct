package csvdistinct

import (
	"encoding/csv"
	"errors"
	"os"
)

// ErrInvalidIDColumn is returned when you pass an invalid idColumn in. Should be a string or integer
var ErrInvalidIDColumn = errors.New("Invalid Id Column")

// ErrIDColumnMissing is returned when a valid column is passed, but not found
var ErrIDColumnMissing = errors.New("Can't find Id Column")

// Config contains any options for reading the CSV
type Config struct {
	HasHeader        bool
	Comma            rune
	Comment          rune
	FieldsPerRecord  int
	LazyQuotes       bool
	TrailingComma    bool
	TrimLeadingSpace bool
}

// CSVDistinct is a struct to contain the Config
type CSVDistinct struct {
	Config Config
}

// Callback is called for all matching distinct values
type Callback func([][]string)

// NewCSVDistinctReader creates a new instance
func NewCSVDistinctReader() *CSVDistinct {
	ret := new(CSVDistinct)
	ret.Config.Comma = ','
	return ret
}

// ReadCSV opens the file and scans the document for unique values in the idColumn.
// After the document has been scanned, the onDistinct is called with the matching values
// based on the id column header. idColumn can be a string or integer. If using a string,
// the document must have a header to match.
func (v CSVDistinct) ReadCSV(file string, idColumn interface{}, onDistinct Callback) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	rdr := csv.NewReader(f)
	rdr.Comma = v.Config.Comma
	rdr.Comment = v.Config.Comment
	rdr.FieldsPerRecord = v.Config.FieldsPerRecord
	rdr.LazyQuotes = v.Config.LazyQuotes
	rdr.TrailingComma = v.Config.TrailingComma
	rdr.TrimLeadingSpace = v.Config.TrimLeadingSpace

	records, err := rdr.ReadAll()

	if err != nil {
		return err
	}

	column := -1
	startIndex := 0

	if v.Config.HasHeader {
		startIndex = 1
		if intValue, ok := idColumn.(int); ok {
			column = intValue
		} else if strValue, ok := idColumn.(string); ok {
			for i, v := range records[0] {
				if v == strValue {
					column = i
				}
			}

			if column == -1 {
				return ErrIDColumnMissing
			}
		} else {
			return ErrInvalidIDColumn
		}
	} else {
		// We can only take integer if we don't have an integer
		if intValue, ok := idColumn.(int); !ok {
			return ErrInvalidIDColumn
		} else {
			column = intValue
		}
	}

	distinctValues := make(map[string][]int)

	// Go through each row, take the id column and pass the index into an map[string][]int
	for i, v := range records[:startIndex] {
		distinctValues[v[column]] = append(distinctValues[v[column]], i)
	}

	for _, v := range distinctValues {
		var rows [][]string
		for _, rowIndex := range v {
			rows = append(rows, records[rowIndex])
		}

		onDistinct(rows)
	}

	return nil
}
