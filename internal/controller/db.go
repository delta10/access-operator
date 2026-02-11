/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/jackc/pgx/v5"
)

// DBInterface defines the database operations needed by the controller
type DBInterface interface {
	Connect(ctx context.Context, connectionString string) error
	Close(ctx context.Context) error
	CreateUser(ctx context.Context, username, password string) error
	GetUsers(ctx context.Context) ([]string, error)
	GrantPrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error
	RevokePrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error
	GetGrants(ctx context.Context) (map[string][]accessv1.GrantSpec, error)
}

// PostgresDB implements DBInterface using pgx
type PostgresDB struct {
	conn *pgx.Conn
}

type ConnectionDetails struct {
	Username string
	Password string
	Host     string
	Port     string
	Database string
	SSLMode  string
}

func NewPostgresDB() *PostgresDB {
	return &PostgresDB{}
}

func (p *PostgresDB) Connect(ctx context.Context, connectionString string) error {
	conn, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		return err
	}
	p.conn = conn
	return nil
}

func (p *PostgresDB) Close(ctx context.Context) error {
	if p.conn != nil {
		return p.conn.Close(ctx)
	}
	return nil
}

func (p *PostgresDB) CreateUser(ctx context.Context, username, password string) error {
	if p.conn == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	var exists bool
	if err := p.conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)", username).Scan(&exists); err != nil {
		return err
	}

	quotedUsername := pgx.Identifier{username}.Sanitize()
	escapedPassword := strings.ReplaceAll(password, "'", "''")

	var err error
	if exists {
		_, err = p.conn.Exec(ctx, fmt.Sprintf("ALTER ROLE %s WITH LOGIN PASSWORD '%s'", quotedUsername, escapedPassword))
		return err
	}

	_, err = p.conn.Exec(ctx, fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s'", quotedUsername, escapedPassword))
	return err
}

func (p *PostgresDB) GetUsers(ctx context.Context) ([]string, error) {
	if p.conn == nil {
		return nil, fmt.Errorf("database connection is not initialized")
	}

	rows, err := p.conn.Query(ctx, "SELECT rolname FROM pg_roles WHERE rolcanlogin = true and rolsuper = false")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, err
		}
		users = append(users, username)
	}
	return users, nil
}

func (p *PostgresDB) GrantPrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {
	if p.conn == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	quotedUsername := pgx.Identifier{username}.Sanitize()
	for _, grant := range grants {
		schema := "public"
		if grant.Schema != nil && *grant.Schema != "" {
			schema = *grant.Schema
		}

		databasePrivileges := make([]string, 0, len(grant.Privileges))
		schemaPrivileges := make([]string, 0, len(grant.Privileges))
		tablePrivileges := make([]string, 0, len(grant.Privileges))
		functionPrivileges := make([]string, 0, len(grant.Privileges))
		unsupportedPrivileges := make([]string, 0)

		for _, privilege := range grant.Privileges {
			switch strings.ToUpper(privilege) {
			case "CONNECT", "TEMPORARY":
				databasePrivileges = append(databasePrivileges, privilege)
			case "CREATE":
				// CREATE can apply to both DATABASE (create schema) and SCHEMA (create objects).
				databasePrivileges = append(databasePrivileges, privilege)
				schemaPrivileges = append(schemaPrivileges, privilege)
			case "USAGE":
				schemaPrivileges = append(schemaPrivileges, privilege)
			case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN":
				tablePrivileges = append(tablePrivileges, privilege)
			case "EXECUTE":
				functionPrivileges = append(functionPrivileges, privilege)
			default:
				unsupportedPrivileges = append(unsupportedPrivileges, privilege)
			}
		}

		if len(unsupportedPrivileges) > 0 {
			return fmt.Errorf("unsupported privileges: %s", strings.Join(unsupportedPrivileges, ", "))
		}

		quotedDatabase := pgx.Identifier{grant.Database}.Sanitize()
		quotedSchema := pgx.Identifier{schema}.Sanitize()

		if len(databasePrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON DATABASE %s TO %s",
					strings.Join(databasePrivileges, ", "),
					quotedDatabase,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(schemaPrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON SCHEMA %s TO %s",
					strings.Join(schemaPrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(tablePrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA %s TO %s",
					strings.Join(tablePrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}

		if len(functionPrivileges) > 0 {
			_, err := p.conn.Exec(
				ctx,
				fmt.Sprintf("GRANT %s ON ALL FUNCTIONS IN SCHEMA %s TO %s",
					strings.Join(functionPrivileges, ", "),
					quotedSchema,
					quotedUsername,
				),
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *PostgresDB) RevokePrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {

	return nil
}

func (p *PostgresDB) GetGrants(ctx context.Context) (map[string][]accessv1.GrantSpec, error) {
	var dbOutput []struct {
		Grantor       string
		Grantee       string
		Schema        string
		PrivilegeType string
		IsGrantable   bool
	}
	err := p.conn.QueryRow(ctx,
		`SELECT r.usename AS grantor, e.usename AS grantee, nspname, privilege_type, is_grantable 
                FROM pg_namespace, aclexplode(nspacl) AS a
                    JOIN pg_user e ON a.grantee = e.usesysid 
                    JOIN pg_user r ON a.grantor = r.usesysid;`).
		Scan(&dbOutput)
	if err != nil {
		return nil, err
	}

	returnValue := make(map[string][]accessv1.GrantSpec)

	// key is grantee, value is list of grants, first group by grantee, then by schema, then aggregate privileges
	for _, row := range dbOutput {
		grant := accessv1.GrantSpec{
			Database:   "", // we don't have database information in this query, so we leave it empty
			Schema:     &row.Schema,
			Privileges: []string{row.PrivilegeType},
		}
		returnValue[row.Grantee] = append(returnValue[row.Grantee], grant)
	}

	return returnValue, nil
}

// MockDB implements DBInterface for testing
type MockDB struct {
	ConnectCalled         bool
	CreateUserCalled      bool
	GrantPrivilegesCalled bool
	LastConnectionString  string
	LastUsername          string
	LastPassword          string
	LastGrants            []accessv1.GrantSpec
	ConnectError          error
	CreateUserError       error
	GrantPrivilegesError  error
}

func NewMockDB() *MockDB {
	return &MockDB{}
}

func (m *MockDB) Connect(ctx context.Context, connectionString string) error {
	m.ConnectCalled = true
	m.LastConnectionString = connectionString
	return m.ConnectError
}

func (m *MockDB) Close(ctx context.Context) error {
	return nil
}

func (m *MockDB) CreateUser(ctx context.Context, username, password string) error {
	m.CreateUserCalled = true
	m.LastUsername = username
	m.LastPassword = password
	return m.CreateUserError
}

func (m *MockDB) GrantPrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {
	m.GrantPrivilegesCalled = true
	m.LastGrants = grants
	return m.GrantPrivilegesError
}

func (m *MockDB) RevokePrivileges(ctx context.Context, grants []accessv1.GrantSpec, username string) error {
	return nil
}

func (m *MockDB) GetGrants(ctx context.Context) (map[string][]accessv1.GrantSpec, error) {
	return nil, nil
}
