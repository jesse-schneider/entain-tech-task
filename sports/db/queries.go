package db

const (
	eventsList = "list"
	eventsGet  = "get"
)

func getEventQueries() map[string]string {
	return map[string]string{
		eventsList: `
			SELECT 
				id,
				name,  
				visible, 
				advertised_start_time 
			FROM events
		`,
		eventsGet: `
			SELECT 
				id,
				name,
				visible, 
				advertised_start_time 
			FROM events
			WHERE id = ?
		`,
	}
}
