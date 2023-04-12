package main

import (
	"fmt"

	"github.com/craigcogdill/load-balancer/pkg/balance"
)

func main() {
	fmt.Println("hello world")

	b := balance.New(balance.Default)

	b.Start()
}
