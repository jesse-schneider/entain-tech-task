package db

import (
	"testing"
	"time"
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
