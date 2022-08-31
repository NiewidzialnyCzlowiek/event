package slices

func Sum(s []int) int {
	acc := 0
	for i := range s {
		acc += s[i]
	}
	return acc
}
