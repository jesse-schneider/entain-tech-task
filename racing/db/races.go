package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

var (
	invalidOrderByFieldErr = errors.New("invalid order by field")
)

const (
	sortOrderDescLower = "desc"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error)
}

type racesRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewRacesRepo creates a new races repository.
func NewRacesRepo(db *sql.DB) RacesRepo {
	return &racesRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *racesRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy races.
		err = r.seed()
	})

	return err
}

func (r *racesRepo) List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query, args, err = r.applyFilter(query, filter)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanRaces(rows)
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}, error) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args, nil
	}

	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	// Added new filter check here: if visible_races_only is present, add visible = true to WHERE clause.
	// NOTE:: I have extended this as-is for this task, however I would have
	// potentially looked at re-writing this function to use a query builder tool such as squirrel (see: https://github.com/Masterminds/squirrel)
	// why would I re-write? I personally find the builder pattern is far more readable for generating SQL.
	if filter.VisibleRacesOnly {
		clauses = append(clauses, "visible = ?")
		args = append(args, true)
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	// custom order-by has been set we need to build it.
	if filter.OrderBy != "" {
		orderByClause, err := buildAndValidateOrderByClause(filter.OrderBy)
		if err != nil {
			return "", nil, err
		}

		query += orderByClause
	} else {
		query += " ORDER BY advertised_start_time DESC"
	}

	return query, args, nil
}

func (r *racesRepo) scanRaces(rows *sql.Rows) ([]*racing.Race, error) {
	var races []*racing.Race

	for rows.Next() {
		var race racing.Race
		var advertisedStart time.Time

		if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		race.AdvertisedStartTime = ts

		races = append(races, &race)
	}

	return races, nil
}

func buildAndValidateOrderByClause(orderByFilter string) (string, error) {
	// example order_by: meeting_id desc, id
	// each comma separated section is either 1 or 2 "words"
	// blank second section == "asc" sort order
	// more details here: https://cloud.google.com/apis/design/design_patterns#sorting_order

	var orderByClause string
	t := reflect.TypeOf(racing.Race{})
	orderByClause = " ORDER BY "

	//split order-by fields by comma, into "words" made up of either field name or field name + "desc"
	orderBySplit := strings.Split(orderByFilter, ",")
	for i, field := range orderBySplit {
		word := strings.Split(strings.TrimLeft(strings.TrimRight(field, " "), " "), " ")
		switch {
		case len(word) == 1:
			// validate that the requested order by field exists using reflection
			_, ok := t.FieldByNameFunc(func(s string) bool {
				return strings.ToLower(s) == strings.ReplaceAll(word[0], "_", "")
			})
			if !ok {
				return "", invalidOrderByFieldErr
			}

			orderByClause += fmt.Sprintf("%s ASC", word[0])

		case len(word) == 2:
			// ensure the second "word" is desc, if it isn't we want to error now
			if strings.ToLower(word[1]) != sortOrderDescLower {
				return "", invalidOrderByFieldErr
			}

			// validate that the requested order by field exists using reflection
			_, ok := t.FieldByNameFunc(func(s string) bool {
				return strings.ToLower(s) == strings.ReplaceAll(word[0], "_", "")
			})
			if !ok {
				return "", invalidOrderByFieldErr
			}
			orderByClause += fmt.Sprintf("%s DESC", word[0])

		default:
			// default error case to capture anything that isn't our 2 success cases
			return "", invalidOrderByFieldErr
		}

		// add comma to clause when we have more terms to add
		if i != len(orderBySplit)-1 {
			orderByClause += ", "
		}
	}

	return orderByClause, nil
}