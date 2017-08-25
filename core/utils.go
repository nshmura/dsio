package core

import (
	"fmt"
	"strconv"
	"strings"

	"bufio"
	"os"

	"cloud.google.com/go/datastore"
)

func ToString(value interface{}) string {
	return fmt.Sprintf("%v", value)
}

func ToFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	}
	return 0, fmt.Errorf("can't convert %v to float64", val)
}

func KeyToString(k *datastore.Key) string {

	if k.Parent == nil {
		if k.ID != 0 {
			return strconv.FormatInt(k.ID, 10)
		} else {
			return strconv.Quote(k.Name)
		}
	}

	keys := make([]string, 0)

	for {
		var v string
		if k.ID != 0 {
			v = strconv.FormatInt(k.ID, 10)
		} else {
			v = strconv.Quote(k.Name)
		}
		keys = append([]string{strconv.Quote(k.Kind), v}, keys...)

		if k.Parent == nil {
			return "[" + strings.Join(keys, ",") + "]"
		}

		k = k.Parent
	}
}

func ConfirmYesNo(msg string) (bool, error) {

	reader := bufio.NewReader(os.Stdin)

	for {
		Conform(msg + " (y/n):")

		answer, err := reader.ReadString('\n')
		if err != nil {
			return false, err
		}

		switch strings.ToUpper(strings.Trim(answer, "\n")) {
		case "Y":
			fmt.Println("")
			return true, nil
		case "N":
			fmt.Println("")
			return false, nil
		default:
			// confirm once more
		}
	}
}

func ConfirmYesNoWithDefault(msg string, defaultValue bool) (bool, error) {

	reader := bufio.NewReader(os.Stdin)
	for {
		var confirmStr string
		if defaultValue {
			confirmStr = " (Y/n): "
		} else {
			confirmStr = " (y/N): "
		}

		Conform(msg + confirmStr)

		answer, err := reader.ReadString('\n')
		if err != nil {
			return false, err
		}

		switch strings.ToUpper(strings.Trim(answer, "\n")) {
		case "Y":
			fmt.Println("")
			return true, nil
		case "N":
			fmt.Println("")
			return false, nil
		case "":
			fmt.Println("")
			return defaultValue, nil
		default:
			// confirm once more
		}
	}
}
