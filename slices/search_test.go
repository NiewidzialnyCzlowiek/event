package slices

import (
	"reflect"
	"testing"
)

func TestContainsNumber(t *testing.T) {
	type args struct {
		slice   []int
		element int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Slice contains existing int",
			args: args{
				slice:   []int{1, 2, 3, 4, 5, 6},
				element: 3,
			},
			want: true,
		},
		{
			name: "Slice doeas not contain non-existing int",
			args: args{
				slice:   []int{1, 2, 3, 4, 5, 6},
				element: 10,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Contains(tt.args.slice, tt.args.element); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContainsString(t *testing.T) {
	type args struct {
		slice   []string
		element string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Slice contains existing string",
			args: args{
				slice:   []string{"ala", "ma", "kota"},
				element: "kota",
			},
			want: true,
		},
		{
			name: "Slice doeas not contain non-existing string",
			args: args{
				slice:   []string{"ala", "ma", "kota"},
				element: "psa",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Contains(tt.args.slice, tt.args.element); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilterNumber(t *testing.T) {
	type args struct {
		slice     []int
		predicate func(int) bool
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{
			name: "Filter single element from slice",
			args: args{
				slice:     []int{1, 2, 3, 4},
				predicate: func(x int) bool { return x != 3 },
			},
			want: []int{1, 2, 4},
		},
		{
			name: "Filter no elements from slice",
			args: args{
				slice:     []int{1, 2, 3, 4},
				predicate: func(x int) bool { return true },
			},
			want: []int{1, 2, 3, 4},
		},
		{
			name: "Filter all elements from slice",
			args: args{
				slice:     []int{1, 2, 3, 4},
				predicate: func(x int) bool { return false },
			},
			want: []int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Filter(tt.args.slice, tt.args.predicate); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}
