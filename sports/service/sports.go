package service

import (
	"git.neds.sh/matty/entain/sports/db"
	"git.neds.sh/matty/entain/sports/proto/sports"
	"golang.org/x/net/context"
)

type Sporting interface {
	// ListSports will return a collection of sports.
	ListSports(ctx context.Context, in *sports.ListSportsRequest) (*sports.ListSportsResponse, error)
	FetchSport(ctx context.Context, in *sports.FetchSportRequest) (*sports.FetchSportResponse, error)
}

// sportsService implements the Sport interface.
type sportService struct {
	sportsRepo db.SportsRepo
}

// NewSportService instantiates and returns a new sportService.
func NewSportService(sportsRepo db.SportsRepo) Sporting {
	return &sportService{sportsRepo}
}

func (s *sportService) ListSports(ctx context.Context, in *sports.ListSportsRequest) (*sports.ListSportsResponse, error) {
	sportList, err := s.sportsRepo.List(in.Filter)
	if err != nil {
		return nil, err
	}
	return &sports.ListSportsResponse{Sports: sportList}, nil
}

func (s *sportService) FetchSport(ctx context.Context, in *sports.FetchSportRequest) (*sports.FetchSportResponse, error) {
	sport, err := s.sportsRepo.FetchSport(in)
	if err != nil {
		return nil, err
	}

	return &sports.FetchSportResponse{Sport: sport}, nil
}
