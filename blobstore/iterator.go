package blobstore

import "github.com/ZwickyTransientFacility/alertbase/schema"

// AlertIterator provides access to a stream of alert messages.
//
// Example usage:
// for ai.Next() {
//     fmt.Println(ai.Value()
// }
// err := ai.Error()
// if err != nil {
//     handle(err)
// }
type AlertIterator struct {
	more    bool
	current *schema.Alert
	err     error

	alerts chan *schema.Alert
	errors chan error
}

// Next advances the iterator to the next alert message. It returns true if
// ai.Value() will return a valid alert message. If it returns false, then the
// caller should check ai.Error() for a non-nil value.
func (ai *AlertIterator) Next() bool {
	select {
	case a, ok := <-ai.alerts:
		if !ok {
			ai.current = nil
			return false
		}
		ai.current = a
		return true
	case err, ok := <-ai.errors:
		if !ok {
			ai.current = nil
			return false
		}
		ai.err = err
		return false
	}
}

// Value returns the alert at the current position of the iterator. It can be
// called multiple times without advancing.
func (ai *AlertIterator) Value() *schema.Alert {
	return ai.current
}

// Error returns the first error in the stream, if there is one.
func (ai *AlertIterator) Error() error {
	return ai.err
}
