package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_calculateRaceStatus(t *testing.T) {
	tests := []struct {
		name           string
		advertisedTime time.Time
		want           string
	}{
		{
			name:           "OPEN: future time",
			advertisedTime: time.Now().Add(1 * time.Minute),
			want:           raceStatusOpen,
		},
		{
			name:           "CLOSED: now",
			advertisedTime: time.Now(),
			want:           raceStatusClosed,
		},
		{
			name:           "CLOSED: past time",
			advertisedTime: time.Now().Add(-1 * time.Minute),
			want:           raceStatusClosed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateRaceStatus(tt.advertisedTime); got != tt.want {
				t.Errorf("calculateRaceStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buildAndValidateOrderByClause(t *testing.T) {
	tests := []struct {
		name          string
		orderByFilter string
		want          string
		wantErr       assert.ErrorAssertionFunc
	}{
		{
			name:          "success: standard format, 1 column",
			orderByFilter: "meeting_id desc",
			want:          " ORDER BY meeting_id DESC",
			wantErr:       assert.NoError,
		},
		{
			name:          "success: standard format, 2 columns",
			orderByFilter: "meeting_id, id desc",
			want:          " ORDER BY meeting_id ASC, id DESC",
			wantErr:       assert.NoError,
		},
		{
			name:          "fail: capital letters",
			orderByFilter: "Name, MeetingId desc",
			want:          "",
			wantErr:       assert.Error,
		},
		{
			name:          "fail: incorrect field",
			orderByFilter: "some-order-junk",
			want:          "",
			wantErr:       assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildAndValidateOrderByClause(tt.orderByFilter)
			if !tt.wantErr(t, err, "buildAndValidateOrderByClause()") {
				return
			}
			assert.Equalf(t, tt.want, got, "buildAndValidateOrderByClause()")
		})
	}
}
