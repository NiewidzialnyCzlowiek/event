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
	type want struct {
		index  int
		exists bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "Slice contains existing int",
			args: args{
				slice:   []int{1, 2, 3, 4, 5, 6},
				element: 3,
			},
			want: want{2, true},
		},
		{
			name: "Slice doeas not contain non-existing int",
			args: args{
				slice:   []int{1, 2, 3, 4, 5, 6},
				element: 10,
			},
			want: want{0, false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if index, exists := Contains(tt.args.slice, tt.args.element); index != tt.want.index || exists != tt.want.exists {
				t.Errorf("Contains() = %v, want %v", want{index, exists}, tt.want)
			}
		})
	}
}

func TestContainsString(t *testing.T) {
	type args struct {
		slice   []string
		element string
	}
	type want struct {
		index  int
		exists bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "Slice contains existing string",
			args: args{
				slice:   []string{"ala", "ma", "kota"},
				element: "kota",
			},
			want: want{2, true},
		},
		{
			name: "Slice doeas not contain non-existing string",
			args: args{
				slice:   []string{"ala", "ma", "kota"},
				element: "psa",
			},
			want: want{0, false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if index, exists := Contains(tt.args.slice, tt.args.element); index != tt.want.index || exists != tt.want.exists {
				t.Errorf("Contains() = %v, want %v", want{index, exists}, tt.want)
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
