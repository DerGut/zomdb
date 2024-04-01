package zomdb_test

import (
	"context"
	"fmt"

	"github.com/DerGut/zomdb"
)

func Example() {
	db, err := zomdb.New()
	if err != nil {
		panic(err)
	}

	if err := db.Set(context.Background(), []byte("key"), []byte("value")); err != nil {
		panic(err)
	}

	value, err := db.Get(context.Background(), []byte("key"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("%q\n", value)
	// Output: "value"
}
