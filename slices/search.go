package slices

func Contains[T comparable](slice []T, element T) bool {
	for i := range slice {
		if slice[i] == element {
			return true
		}
	}
	return false
}

func Filter[T any](slice []T, predicate func(T) bool) []T {
	result := make([]T, 0, len(slice))
	for i := range slice {
		if predicate(slice[i]) {
			result = append(result, slice[i])
		}
	}
	return result
}
