package main

import (
	"strings"
	"testing"

	"go.arcalot.io/assert"
	"gopkg.in/yaml.v3"
)

const (
	schemaString = `
steps:
    create:
        id: create
        input:
            objects:
                Connection:
                    id: Connection
                    properties:
                        bearerToken:
                            type:
                                type_id: string
`
	schemaInt = `
steps:
    create:
        id: create
        input:
            objects:
                Connection:
                    id: Connection
                    properties:
                        burst:
                            type:
                                type_id: integer
`
	schemaFloat = `
steps:
    create:
        id: create
        input:
            objects:
                Connection:
                    id: Connection
                    properties:
                        qps:
                            type:
                                type_id: float
`
	schemaRef = `
steps:
    create:
        id: create
        input:
            objects:
                Connection:
                    id: Connection
                    properties:
                        metadata:
                            type:
                                id: ObjectMeta
                                type_id: ref
`
	expectedString = `package arcaflow_plugin_service

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Connection struct {
	BearerToken string ` + "`" + `json:"bearerToken"` + "`" + `
}
`
	expectedInt = `package arcaflow_plugin_service

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Connection struct {
	Burst int64 ` + "`" + `json:"burst"` + "`" + `
}
`
	expectedFloat = `package arcaflow_plugin_service

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Connection struct {
	Qps float64 ` + "`" + `json:"qps"` + "`" + `
}
`
	expectedRef = `package arcaflow_plugin_service

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Connection struct {
	Metadata ObjectMeta ` + "`" + `json:"metadata"` + "`" + `
}
`
)

func TestGenerateTypeDefString(t *testing.T) {
	var schema schema
	err := yaml.Unmarshal([]byte(schemaString), &schema)
	check(err)

	gotBytes := mustGenerateTypeDef(schema)
	got := removeHeader(string(gotBytes))
	assert.Equals(t, strings.TrimSpace(got), strings.TrimSpace(expectedString))
}

func TestGenerateTypeDefInt(t *testing.T) {
	var schema schema
	err := yaml.Unmarshal([]byte(schemaInt), &schema)
	check(err)

	gotBytes := mustGenerateTypeDef(schema)
	got := removeHeader(string(gotBytes))
	assert.Equals(t, strings.TrimSpace(got), strings.TrimSpace(expectedInt))
}

func TestGenerateTypeDefFloat(t *testing.T) {
	var schema schema
	err := yaml.Unmarshal([]byte(schemaFloat), &schema)
	check(err)

	gotBytes := mustGenerateTypeDef(schema)
	got := removeHeader(string(gotBytes))
	assert.Equals(t, strings.TrimSpace(got), strings.TrimSpace(expectedFloat))
}

func TestGenerateTypeDefRef(t *testing.T) {
	var schema schema
	err := yaml.Unmarshal([]byte(schemaRef), &schema)
	check(err)

	gotBytes := mustGenerateTypeDef(schema)
	got := removeHeader(string(gotBytes))
	assert.Equals(t, strings.TrimSpace(got), strings.TrimSpace(expectedRef))
}

func removeHeader(s string) string {
	i := strings.IndexByte(s, '\n')
	if i == -1 {
		return s
	}
	return s[i+1:]
}
