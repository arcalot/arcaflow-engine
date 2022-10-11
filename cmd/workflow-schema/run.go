package main

import (
	"fmt"

	"go.arcalot.io/lang"
	"go.flow.arcalot.io/engine/workflow"
	"gopkg.in/yaml.v3"
)

func main() {
	s := workflow.GetSchema()
	fmt.Printf("%s", lang.Must2(yaml.Marshal(lang.Must2(s.SelfSerialize()))))
}
