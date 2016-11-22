package bigquery

import (
	//	"code.uber.internal/go-common.git/x/log"
	"google.golang.org/api/iterator"
)

// MapLoader is a ValueLoader that loads into a slice of maps to values.
// Don't reuse a MapLoader - Changing the schema will invalidate the convenience functions on the rows. That could be ugly.
type MapLoader struct {
	Schema Schema
	// The current field number.
	Fields   map[string]FieldSchema
	Rows     []*MapLoaderRow
	saveLast bool
}

// Row is a map of field names to their values.
type Row map[string]Value

// A MapLoaderRow is a convenience type to allow us to access our fields in a sane way.
type MapLoaderRow struct {
	Row
	m *MapLoader
}

func newRow(m *MapLoader) *MapLoaderRow {
	return &MapLoaderRow{
		m:   m,
		Row: Row{},
	}
}

//Convenience function to handle conversions. Should have versions for each FieldType
func (r *MapLoaderRow) fieldAsString(fieldName string) string {
	if r.m.Fields[fieldName].Type != StringFieldType {
		panic("Field Type mismatch!!")
	}
	return r.Row[fieldName].(string)
}

// Load is a basic implementation of loader.
func (m *MapLoader) Load(v []Value, s Schema) error {
	if !m.saveLast {
		//Load the fields into a name-mapped map of FieldSchemas
		m.Fields = make(map[string]FieldSchema)
		for _, f := range s {
			m.Fields[f.Name] = *f
		}
		m.Rows = nil
	}
	// The current row we're adding values to.
	var r *MapLoaderRow
	// Our very first field needs to trigger the make, so let's just do that.
	curfield := len(s)
	for _, val := range v {
		curfield++
		if curfield >= len(s) {
			curfield = 0
			r = newRow(m)
			m.Rows = append(m.Rows, r)
		}
		r.Row[s[curfield].Name] = val
	}

	return nil
}

//LoadFromIterator takes an iterator and keeps loading. Don't do this with non-fresh maploaders.
func (m *MapLoader) LoadFromIterator(it ValueIterator) error {
	m.saveLast = true
	for {
		err := it.Next(m)
		if err == iterator.Done {
			m.saveLast = false
			return nil
		}
		if err != nil {
			return err
		}
	}
}
