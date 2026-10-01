package main

import (
	"context"
	"net/http"
	"strings"
)

type ctxUserKey string

const (
	userIDKey   ctxUserKey = "user_id"
	userRoleKey ctxUserKey = "user_role"
)

func (a *authServer) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "missing bearer token")
			return
		}
		tokenStr := strings.TrimPrefix(header, prefix)

		c, err := parseToken(a.jwtSecret, tokenStr, "access")
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid or expired token")
			return
		}

		ctx := context.WithValue(r.Context(), userIDKey, c.Subject)
		ctx = context.WithValue(ctx, userRoleKey, c.Role)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requireRole(role string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Value(userRoleKey) != role {
			writeError(w, http.StatusForbidden, "forbidden", "wrong role")
			return
		}
		next.ServeHTTP(w, r)
	})
}
