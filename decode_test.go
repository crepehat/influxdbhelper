package influxdbhelper

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestDecode(t *testing.T) {
	columns := []string{
		"intValue",
		"floatValue",
		"boolValue",
		"stringValue",
	}

	_ = columns

	values := [][]interface{}{}

	type DecodeType struct {
		IntValue    int     `influx:"intValue"`
		FloatValue  float64 `influx:"floatValue"`
		BoolValue   bool    `influx:"boolValue"`
		StringValue string  `influx:"stringValue"`
	}

	expected := []DecodeType{}

	for i := 0; i < 10; i++ {
		v := DecodeType{
			i,
			float64(i),
			math.Mod(float64(i), 2) == 0,
			strconv.Itoa(i),
		}

		v_ := []interface{}{
			v.IntValue,
			v.FloatValue,
			v.BoolValue,
			v.StringValue,
		}

		expected = append(expected, v)
		values = append(values, v_)

	}

	decoded := []DecodeType{}

	err := Decode(columns, values, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeMissingColumn(t *testing.T) {
	columns := []string{
		"val1",
	}

	_ = columns

	type DecodeType struct {
		Val1 int `influx:"val1"`
		Val2 int `influx:"val2"`
	}

	expected := []DecodeType{{1, 0}}

	values := [][]interface{}{{1}}

	decoded := []DecodeType{}

	err := Decode(columns, values, &decoded)
	if err == nil {
		t.Error("Expected error decoding: ", err)
	} else {
		fmt.Println("Got expected error: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeWrongType(t *testing.T) {
	columns := []string{
		"val1", "val2",
	}

	_ = columns

	type DecodeType struct {
		Val1 int     `influx:"val1"`
		Val2 float64 `influx:"val2"`
	}

	expected := []DecodeType{}

	values := [][]interface{}{{1.0, 2}}

	decoded := []DecodeType{}

	err := Decode(columns, values, &decoded)
	if err == nil {
		t.Error("Expected error decoding: ", err)
	} else {
		fmt.Println("Got expected error: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeTime(t *testing.T) {
	columns := []string{
		"time", "value",
	}

	_ = columns

	type DecodeType struct {
		Time  time.Time `influx:"time"`
		Value float64   `influx:"value"`
	}

	timeS := "2018-06-14T21:47:11Z"
	time, err := time.Parse(time.RFC3339, timeS)
	if err != nil {
		t.Error("error parsing expected time: ", err)
	}

	expected := []DecodeType{{time, 2.0}}

	values := [][]interface{}{{timeS, 2.0}}

	decoded := []DecodeType{}

	err = Decode(columns, values, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeJsonNumber(t *testing.T) {
	columns := []string{
		"val1", "val2",
	}

	_ = columns

	type DecodeType struct {
		Val1 int     `influx:"val1"`
		Val2 float64 `influx:"val2"`
	}

	expected := []DecodeType{{1, 2.0}}

	values := [][]interface{}{{json.Number("1"), json.Number("2.0")}}

	decoded := []DecodeType{}

	err := Decode(columns, values, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeUnsedStructValue(t *testing.T) {
	columns := []string{
		"val1", "val2",
	}

	_ = columns

	type DecodeType struct {
		Val1 int     `influx:"val1"`
		Val2 float64 `influx:"-"`
	}

	expected := []DecodeType{{1, 0}}

	values := [][]interface{}{{1}}

	decoded := []DecodeType{}

	err := Decode(columns, values, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}