package utils

import "fmt"

// HelloWorld returns a simple greeting message
func HelloWorld() string {
	return "Hello from utils module!"
}

// PrintHelloWorld prints the hello world message to stdout
func PrintHelloWorld() {
	fmt.Println(HelloWorld())
}
