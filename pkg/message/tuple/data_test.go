package tuple

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeWithColumn_Text(t *testing.T) {
	d := &Data{
		ColumnNumber: 2,
		Columns: DataColumns{
			{DataType: DataTypeText, Data: []byte("605")},
			{DataType: DataTypeText, Data: []byte("foo")},
		},
	}
	columns := []RelationColumn{
		{Name: "id", DataType: 23},   // int4
		{Name: "name", DataType: 25}, // text
	}

	decoded, err := d.DecodeWithColumn(columns)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": int32(605), "name": "foo"}, decoded)
}

func TestDecodeWithColumn_Binary(t *testing.T) {
	// int4 605 encoded big-endian, as PostgreSQL sends in binary format.
	d := &Data{
		ColumnNumber: 1,
		Columns: DataColumns{
			{DataType: DataTypeBinary, Data: []byte{0x00, 0x00, 0x02, 0x5d}},
		},
	}
	columns := []RelationColumn{
		{Name: "id", DataType: 23}, // int4
	}

	decoded, err := d.DecodeWithColumn(columns)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": int32(605)}, decoded)
}

func TestDecodeWithColumn_BinaryNumeric(t *testing.T) {
	// numeric 605 encoded in PostgreSQL's binary numeric wire format:
	// ndigits=1, weight=0, sign=0x0000 (positive), dscale=0, digits=[605].
	binNumeric := []byte{
		0x00, 0x01, // ndigits = 1
		0x00, 0x00, // weight = 0
		0x00, 0x00, // sign = positive
		0x00, 0x00, // dscale = 0
		0x02, 0x5d, // digit[0] = 605
	}
	d := &Data{
		ColumnNumber: 1,
		Columns: DataColumns{
			{DataType: DataTypeBinary, Data: binNumeric},
		},
	}
	columns := []RelationColumn{
		{Name: "total_volume", DataType: 1700}, // numeric
	}

	decoded, err := d.DecodeWithColumn(columns)
	require.NoError(t, err)
	require.Contains(t, decoded, "total_volume")
}

func TestDecodeWithColumn_NullAndUnchangedToastOmitted(t *testing.T) {
	d := &Data{
		ColumnNumber: 2,
		Columns: DataColumns{
			{DataType: DataTypeNull},
			{DataType: DataTypeToast},
		},
	}
	columns := []RelationColumn{
		{Name: "deleted_at", DataType: 1114},
		{Name: "big_blob", DataType: 25},
	}

	decoded, err := d.DecodeWithColumn(columns)
	require.NoError(t, err)
	assert.Nil(t, decoded["deleted_at"])
	_, hasBigBlob := decoded["big_blob"]
	assert.False(t, hasBigBlob, "unchanged TOAST column must be omitted, not zero-valued")
}
