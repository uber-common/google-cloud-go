package bigquery

import (
	"reflect"
	"testing"

	"google.golang.org/api/iterator"
)

var row1 = Row{
	"Name":      "foo",
	"Age":       3,
	"GPA":       4.0,
	"LastLogin": 5}

// Our test data.
var row2 = Row{
	"Name":      "bar",
	"Age":       10,
	"GPA":       2.5,
	"LastLogin": 1477952793}

var mapSchema = Schema{
	&FieldSchema{Name: "Name", Type: StringFieldType},
	&FieldSchema{Name: "Age", Type: IntegerFieldType},
	&FieldSchema{Name: "GPA", Type: FloatFieldType},
	&FieldSchema{Name: "LastLogin", Type: TimestampFieldType},
}

// Test the basics of loading data into a MapLoader
func TestMapLoad(t *testing.T) {
	values := []Value{
		"foo", 3, 4.0, 5,
		"bar", 10, 2.5, 1477952793}

	var m MapLoader

	m.Load(values, mapSchema)

	expected := []*MapLoaderRow{{row1, &m}, {row2, &m}}

	if len(m.Rows) != len(expected) {
		t.Fail()
	}
	for i := range m.Rows {
		if !reflect.DeepEqual(m.Rows[i], expected[i]) {
			t.Error(m.Rows, expected)
		}
	}
}

// A simple iterator implementation.
type iteratorForTesting struct {
	valueRows [][]Value
	row       *int
}

// The actual implementation
func (i iteratorForTesting) Next(n ValueLoader) error {
	if (*i.row) >= len(i.valueRows) {
		return iterator.Done
	}
	n.Load(i.valueRows[(*i.row)], mapSchema)
	(*i.row)++
	return nil
}

// This function should test that the maploader gets the full set of values even if they're broken into small chunks by pagination.
func TestMultipageIterator(t *testing.T) {
	var m MapLoader
	values := []Value{
		"foo", 3, 4.0, 5}
	values2 := []Value{
		"bar", 10, 2.5, 1477952793}
	var it iteratorForTesting
	it.valueRows = [][]Value{values, values2}
	it.row = new(int)

	expected := []*MapLoaderRow{{row1, &m}, {row2, &m}}

	m.LoadFromIterator(it)

	if len(m.Rows) != len(expected) {
		t.Fatal(len(m.Rows), len(expected))
	}
	for i := range m.Rows {
		if !reflect.DeepEqual(m.Rows[i], expected[i]) {
			t.Fatal(m.Rows, expected)
		}
	}
}
