package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type authServer struct {
	pool      *pgxpool.Pool
	jwtSecret []byte
}

type claims struct {
	Role string `json:"role"`
	Typ  string `json:"typ"`
	jwt.RegisteredClaims
}

func issueToken(secret []byte, userID, role, typ string, ttl time.Duration) (string, error) {
	now := time.Now()
	c := claims{
		Role: role,
		Typ:  typ,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	return token.SignedString(secret)
}

func issueTokenPair(secret []byte, userID, role string) (access, refresh string, err error) {
	access, err = issueToken(secret, userID, role, "access", 5*time.Minute)
	if err != nil {
		return "", "", err
	}
	refresh, err = issueToken(secret, userID, role, "refresh", 7*24*time.Hour)
	if err != nil {
		return "", "", err
	}
	return access, refresh, nil
}

type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

type userResponse struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

type authResponse struct {
	AccessToken  string        `json:"access_token"`
	RefreshToken string        `json:"refresh_token"`
	User         *userResponse `json:"user,omitempty"`
}

func (a *authServer) register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", "invalid json body")
		return
	}

	var fields []fieldError
	if req.Email == "" {
		fields = append(fields, fieldError{"email", "must not be empty"})
	}
	if req.Password == "" {
		fields = append(fields, fieldError{"password", "must not be empty"})
	}
	if req.Role != "buyer" && req.Role != "seller" {
		fields = append(fields, fieldError{"role", "must be buyer or seller"})
	}
	if len(fields) > 0 {
		writeError(w, http.StatusBadRequest, "validation_failed", "request body is invalid", fields...)
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "hash password")
		return
	}

	id := uuid.NewString()
	_, err = a.pool.Exec(r.Context(), `
		INSERT INTO users (id, email, password_hash, role) VALUES ($1, $2, $3, $4)
	`, id, req.Email, string(hash), req.Role)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			writeError(w, http.StatusConflict, "conflict", "email already registered")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", "create user")
		return
	}

	access, refresh, err := issueTokenPair(a.jwtSecret, id, req.Role)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "issue tokens")
		return
	}

	writeJSON(w, http.StatusCreated, authResponse{
		AccessToken:  access,
		RefreshToken: refresh,
		User:         &userResponse{ID: id, Email: req.Email, Role: req.Role},
	})
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (a *authServer) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", "invalid json body")
		return
	}

	var (
		id           string
		passwordHash string
		role         string
	)
	err := a.pool.QueryRow(r.Context(), `
		SELECT id, password_hash, role FROM users WHERE email = $1
	`, req.Email).Scan(&id, &passwordHash, &role)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid email or password")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid email or password")
		return
	}

	access, refresh, err := issueTokenPair(a.jwtSecret, id, role)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "issue tokens")
		return
	}

	writeJSON(w, http.StatusOK, authResponse{AccessToken: access, RefreshToken: refresh})
}
func parseToken(secret []byte, tokenStr, expectedTyp string) (*claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &claims{}, func(t *jwt.Token) (any, error) {
		return secret, nil
	})
	if err != nil {
		return nil, err
	}
	c, ok := token.Claims.(*claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	if c.Typ != expectedTyp {
		return nil, fmt.Errorf("wrong token type")
	}
	return c, nil
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func (a *authServer) refresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", "invalid json body")
		return
	}

	c, err := parseToken(a.jwtSecret, req.RefreshToken, "refresh")
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid or expired refresh token")
		return
	}

	access, refresh, err := issueTokenPair(a.jwtSecret, c.Subject, c.Role)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "issue tokens")
		return
	}

	writeJSON(w, http.StatusOK, authResponse{AccessToken: access, RefreshToken: refresh})
}
