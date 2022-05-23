package service

import (
	"database/sql"
	"log"
	"testing"

	rdb "git.neds.sh/matty/entain/racing/db"
	"git.neds.sh/matty/entain/racing/proto/racing"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type applyFilterConfig struct {
	MeetingIDs          []int64
	OnlyVisible         bool
	DBRaces             []*racing.Race
	ExpectedRaces       []*racing.Race
	ExpectedQueryString string
}

func Test_applyFilter(t *testing.T) {
	configs := map[string]applyFilterConfig{
		"OnlyVisible is true, 2 races expected.": {
			MeetingIDs:  []int64{1, 2},
			OnlyVisible: true,
			DBRaces: []*racing.Race{
				{
					Id:                  1,
					MeetingId:           1,
					Name:                "Test",
					Number:              2,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 1},
				},
				{
					Id:                  2,
					MeetingId:           2,
					Name:                "Test2",
					Number:              5,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 2},
				},
			},
			ExpectedRaces: []*racing.Race{
				{
					Id:                  1,
					MeetingId:           1,
					Name:                "Test",
					Number:              2,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 1},
				},
				{
					Id:                  2,
					MeetingId:           2,
					Name:                "Test2",
					Number:              5,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 2},
				},
			},
			ExpectedQueryString: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE visible = true",
		},
		"OnlyVisible is false, 2 races expected.": {
			MeetingIDs:  []int64{1, 2},
			OnlyVisible: false,
			DBRaces: []*racing.Race{
				{
					Id:                  1,
					MeetingId:           1,
					Name:                "Test",
					Number:              2,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 1},
				},
				{
					Id:                  2,
					MeetingId:           2,
					Name:                "Test2",
					Number:              5,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 2},
				},
			},
			ExpectedRaces: []*racing.Race{
				{
					Id:                  1,
					MeetingId:           1,
					Name:                "Test",
					Number:              2,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 1},
				},
				{
					Id:                  2,
					MeetingId:           2,
					Name:                "Test2",
					Number:              5,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 2},
				},
			},
			ExpectedQueryString: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races",
		},
		"OnlyVisible is true, but one race is not visible 1 race expected.": {
			MeetingIDs:  []int64{1, 2},
			OnlyVisible: true,
			DBRaces: []*racing.Race{
				{
					Id:                  1,
					MeetingId:           1,
					Name:                "Test",
					Number:              2,
					Visible:             false,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 1},
				},
				{
					Id:                  2,
					MeetingId:           2,
					Name:                "Test2",
					Number:              5,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 2},
				},
			},
			ExpectedRaces: []*racing.Race{
				{
					Id:                  2,
					MeetingId:           2,
					Name:                "Test2",
					Number:              5,
					Visible:             true,
					AdvertisedStartTime: &timestamppb.Timestamp{Seconds: 2},
				},
			},
			ExpectedQueryString: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE visible = true",
		},
	}
	for _, config := range configs {
		db, mock := NewMock()
		racesRepo := rdb.NewRacesRepo(db)
		for _, race := range config.DBRaces {
			if !config.OnlyVisible || (config.OnlyVisible && race.Visible) {
				rows := sqlmock.NewRows([]string{"id", "meeting_id", "name", "number", "visible", "advertised_start_time"}).
					AddRow(race.Id, race.MeetingId, race.Name, race.Number, race.Visible, race.AdvertisedStartTime.AsTime())
				mock.ExpectQuery(config.ExpectedQueryString).WillReturnRows(rows)
			}
		}
		result, err := racesRepo.List(&racing.ListRacesRequestFilter{OnlyVisible: config.OnlyVisible})
		if err != nil {
			t.Error("received error when none expected")
			t.Error(err)
		}
		for i, gotRace := range result {
			if !proto.Equal(gotRace, config.ExpectedRaces[i]) {
				t.Error("Found result doesn't match expected")
				t.Error(gotRace)
				t.Error(config.ExpectedRaces[i])
			}
		}

	}
}

func NewMock() (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	return db, mock
}
