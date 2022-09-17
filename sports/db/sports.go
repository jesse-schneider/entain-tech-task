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

	"git.neds.sh/matty/entain/sports/proto/sports"
)

var (
	invalidOrderByFieldErr = errors.New("invalid order by field")
	eventNotFoundErr       = errors.New("event not found")
)

const (
	raceStatusClosed   = "CLOSED"
	raceStatusOpen     = "OPEN"
	sortOrderDescLower = "desc"
)

// SportsRepo provides repository access to races.
type SportsRepo interface {
	// Init will initialise our sports repository.
	Init() error

	// List will return a list of events.
	List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error)

	// Get will return a single event, matched by id.
	Get(id string) (*sports.Event, error)
}

type sportsRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewSportsRepo creates a new sports repository.
func NewSportsRepo(db *sql.DB) SportsRepo {
	return &sportsRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *sportsRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy events.
		err = r.seed()
	})

	return err
}

func (r *sportsRepo) List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getEventQueries()[eventsList]

	query, args, err = r.applyFilter(query, filter)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanEvents(rows)
}

// Get will retrieve the event with the matching id, or return a relevant error if it cannot.
func (r *sportsRepo) Get(id string) (*sports.Event, error) {
	var (
		err   error
		query string
	)

	query = getEventQueries()[eventsGet]
	rows, err := r.db.Query(query, id)
	if err != nil {
		return nil, err
	}

	races, err := r.scanEvents(rows)
	if err != nil {
		return nil, err
	}

	if len(races) > 0 {
		return races[0], nil
	}
	return nil, eventNotFoundErr
}

func (r *sportsRepo) applyFilter(query string, filter *sports.ListEventsRequestFilter) (string, []interface{}, error) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args, nil
	}

	if filter.VisibleEventsOnly {
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

func (r *sportsRepo) scanEvents(rows *sql.Rows) ([]*sports.Event, error) {
	var events []*sports.Event

	for rows.Next() {
		var event sports.Event
		var advertisedStart time.Time

		if err := rows.Scan(&event.Id, &event.Name, &event.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		event.Status = calculateEventStatus(advertisedStart)

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		event.AdvertisedStartTime = ts

		events = append(events, &event)
	}

	return events, nil
}

func buildAndValidateOrderByClause(orderByFilter string) (string, error) {
	// example order_by: meeting_id desc, id
	// each comma separated section is either 1 or 2 "words"
	// blank second section == "asc" sort order
	// more details here: https://cloud.google.com/apis/design/design_patterns#sorting_order

	var orderByClause string
	t := reflect.TypeOf(sports.Event{})
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

func calculateEventStatus(advertisedTime time.Time) string {
	if time.Now().After(advertisedTime) {
		return raceStatusClosed
	}
	return raceStatusOpen
}
