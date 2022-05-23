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

	"git.neds.sh/matty/entain/sports/proto/sports"
)

// SportsRepo provides repository access to sports.
type SportsRepo interface {
	// Init will initialise our sports repository.
	Init() error

	// List will return a list of sports.
	List(filter *sports.ListSportsRequestFilter) ([]*sports.Sport, error)
	FetchSport(req *sports.FetchSportRequest) (*sports.Sport, error)
}

type sportsRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewSportsRepo creates a new sports repository.
func NewSportsRepo(db *sql.DB) SportsRepo {
	return &sportsRepo{db: db}
}

// Init prepares the sport repository dummy data.
func (r *sportsRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy sports.
		err = r.seed()
	})

	return err
}

func (r *sportsRepo) List(filter *sports.ListSportsRequestFilter) ([]*sports.Sport, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getSportQueries()[sportsList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	var sports []*sports.Sport

	for rows.Next() {
		sport, err := r.scanSport(rows)
		if err != nil {
			return nil, err
		}
		sports = append(sports, sport)
	}

	return sports, nil
}

func (r *sportsRepo) applyFilter(query string, filter *sports.ListSportsRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

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

	if filter.OrderBy != nil {
		query += fmt.Sprintf(" ORDER BY %s %s", strings.Join(filter.OrderBy.Fields, ", "), sports.OrderBy_Direction_name[int32(*filter.OrderBy.Direction.Enum())])
	}

	return query, args
}

func (m *sportsRepo) scanSport(
	rows *sql.Rows,
) (*sports.Sport, error) {
	sport := new(sports.Sport)
	var advertisedStart time.Time

	if err := rows.Scan(&sport.Id, &sport.MeetingId, &sport.Name, &sport.Number, &sport.Visible, &advertisedStart); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	ts, err := ptypes.TimestampProto(advertisedStart)
	if err != nil {
		return nil, err
	}

	sport.AdvertisedStartTime = ts

	if timestamppb.Now().Seconds >= ts.Seconds {
		sport.Status = sports.Sport_OPEN
	}

	return sport, nil
}

func (r *sportsRepo) FetchSport(req *sports.FetchSportRequest) (*sports.Sport, error) {
	var (
		err   error
		query string
	)

	query = getSportQueries()[sportsList]

	query, err = r.getSportByID(query, req)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}

	rows.Next()
	return r.scanSport(rows)
}

func (r *sportsRepo) getSportByID(query string, req *sports.FetchSportRequest) (string, error) {

	if req == nil || req.Id == "" {
		return query, errors.New("no id was provided")
	}
	query += " WHERE id = " + req.Id
	return query, nil
}
