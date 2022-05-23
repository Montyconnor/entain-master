package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/types/known/timestamppb"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error)
	FetchRace(req *racing.FetchRaceRequest) (*racing.Race, error)
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

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	var races []*racing.Race

	for rows.Next() {
		race, err := r.scanRace(rows)
		if err != nil {
			return nil, err
		}
		races = append(races, race)
	}

	return races, nil
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	// As we only want the filter to show when asking for visible only results we will only want to check visible = true.
	// As when visible = false we show all results, regardless of if they're visible or not.
	if filter.OnlyVisible {
		clauses = append(clauses, "visible = true")
	}

	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	if filter.OrderBy != nil && len(filter.OrderBy.Fields) != 0 {
		query += fmt.Sprintf(
			" ORDER BY %s %s",
			strings.Join(filter.OrderBy.Fields, ", "),
			racing.OrderBy_Direction_name[int32(*filter.OrderBy.Direction.Enum())],
		)
	}

	return query, args
}

func (m *racesRepo) scanRace(
	rows *sql.Rows,
) (*racing.Race, error) {
	race := new(racing.Race)
	var advertisedStart time.Time

	if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
	}

	ts, err := ptypes.TimestampProto(advertisedStart)
	if err != nil {
		return nil, err
	}

	race.AdvertisedStartTime = ts

	if timestamppb.Now().Seconds >= ts.Seconds {
		race.Status = racing.Race_OPEN
	}

	return race, nil
}

func (r *racesRepo) FetchRace(req *racing.FetchRaceRequest) (*racing.Race, error) {
	var (
		err   error
		query string
	)

	query = getRaceQueries()[racesList]

	query, err = r.getRaceByID(query, req)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}

	rows.Next()
	return r.scanRace(rows)
}

func (r *racesRepo) getRaceByID(query string, req *racing.FetchRaceRequest) (string, error) {

	if req == nil || req.Id == "" {
		return query, errors.New("no id was provided")
	}
	query += " WHERE id = " + req.Id
	return query, nil
}
