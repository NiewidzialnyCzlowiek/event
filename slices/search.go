package slices

// Contains searches for the element in the slice.
// It returns an index of the found element and a boolean indicator
// of whether the element was found in the collection.
// If the returned boolean indicator is false, the returned index is 0
// but should not be used.
func Contains[T comparable](slice []T, element T) (int, bool) {
	for i := range slice {
		if slice[i] == element {
			return i, true
		}
	}
	return 0, false
}

// Filter creates a copy of a slice with only the elements
// that satisfy the predicate.
func Filter[T any](slice []T, predicate func(T) bool) []T {
	result := make([]T, 0, len(slice))
	for i := range slice {
		if predicate(slice[i]) {
			result = append(result, slice[i])
		}
	}
	return result
}
