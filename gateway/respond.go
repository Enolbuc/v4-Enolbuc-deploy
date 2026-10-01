package main

import (
	"encoding/json"
	"net/http"
)

type fieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type errorResponse struct {
	Error   string       `json:"error"`
	Message string       `json:"message"`
	Fields  []fieldError `json:"fields,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string, fields ...fieldError) {
	writeJSON(w, status, errorResponse{Error: code, Message: message, Fields: fields})
}
