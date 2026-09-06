package licensemanager

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	errDateEmpty = errors.New("date is empty")
)

func getToday() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
}

func ParseDateFromTime(date string, today time.Time) (time.Time, error) {
	if date == "" {
		return time.Time{}, errDateEmpty
	}

	if strings.EqualFold(date, "today") {
		return getToday(), nil
	}

	if date[0] == '+' {
		date := date[1:]
		var year bool
		var month bool

		if strings.HasSuffix(date, "years") {
			date = strings.Replace(date, "years", "", -1)
			year = true
		} else if strings.HasSuffix(date, "year") {
			date = strings.Replace(date, "year", "", -1)
			year = true
		}

		if strings.HasSuffix(date, "months") {
			date = strings.Replace(date, "months", "", -1)
			month = true
		} else if strings.HasSuffix(date, "month") {
			date = strings.Replace(date, "month", "", -1)
			month = true
		}

		val := 1
		if date != "" {
			num, err := strconv.Atoi(date)
			if err != nil {
				return time.Time{}, err
			}

			val = num
		}

		if month {
			return today.AddDate(0, int(val), -1), nil
		}

		if year {
			return today.AddDate(int(val), 0, -1), nil
		}
	}

	val, err := time.Parse(time.DateOnly, date)
	if err != nil {
		return time.Time{}, err
	}

	return val, nil
}

func ParseDate(date string) (time.Time, error) {
	return ParseDateFromTime(date, getToday())
}
