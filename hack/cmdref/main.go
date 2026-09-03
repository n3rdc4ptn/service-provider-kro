// stub: no CLI commands to document in this fork.
package main

import "os"

func main() {
	if len(os.Args) > 1 {
		_ = os.MkdirAll(os.Args[1], 0o755)
	}
}
