package main

import (
	"strings"
)

// typical usage looks like
//
// var address = util.StringArray{}
// flag.Var(&address, "address", "address (may be given multiple times)")

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *StringArray) String() string {
	return strings.Join(*a, ",")
}
