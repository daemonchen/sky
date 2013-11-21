package core

import (
	"sort"
	"time"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

type EventList []*Event

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Sorting
//--------------------------------------

// Determines the length of an event slice.
func (s EventList) Len() int {
	return len(s)
}

// Compares two events in an event slice.
func (s EventList) Less(i, j int) bool {
	return s[i].Timestamp.Before(s[j].Timestamp)
}

// Swaps two events in an event slice.
func (s EventList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

//--------------------------------------
// Cleaning
//--------------------------------------

func (s EventList) NonEmptyEvents() EventList {
	events := make(EventList, 0)
	for _, event := range s {
		if len(event.Data) > 0 {
			events = append(events, event)
		}
	}
	return events
}

// Normalize rounds all event times to microsecond precision.
func (s EventList) Normalize() EventList {
	for _, event := range s {
		event.Timestamp = event.Timestamp.UTC().Round(time.Microsecond)
	}
	return s
}

// Sort sorts the event in the list by timestamp.
func (s EventList) Sort() EventList {
	sort.Sort(s)
	return s
}

// Dedupe deduplicates events that occur at the same time.
func (s EventList) Dedupe() EventList {
	m := make(map[time.Time]*Event)
	events := make(EventList, 0)
	for _, e := range s {
		if m[e.Timestamp] == nil {
			events = append(events, e)
			m[e.Timestamp] = e
		}
	}
	return events
}

// Merge takes a list of new events and overlays them onto an existing list.
func (s EventList) Merge(newEvents EventList) EventList {
	events := s.Normalize().Dedupe()
	newEvents = newEvents.Normalize().Dedupe()

	// Create a lookup of existing events by timestamp.
	m := make(map[time.Time]*Event)
	for _, e := range events {
		m[e.Timestamp] = e
	}

	// Loop over new events and insert or merge.
	for _, newEvent := range newEvents {
		if e := m[newEvent.Timestamp]; e != nil {
			e.Merge(newEvent)
		} else {
			events = append(events, newEvent)
		}
	}

	return events.Sort()
}
